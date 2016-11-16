# -*- coding: utf-8 -*-
# Author Karol Sikora <karol.sikora@laboratorium.ee>, (c) 2012
# Author Michal Nowotka <mmmnow@gmail.com>, (c) 2013-2014

try:
    import cPickle as pickle
except ImportError:
    import pickle
import base64
import pymongo
from datetime import datetime, timedelta
from django.core.cache.backends.base import BaseCache
import zlib
import logging
try:
    from urllib import urlencode
except ImportError:
    from urllib.parse import urlencode

#-----------------------------------------------------------------------------------------------------------------------

MAX_SIZE = 16000000

#-----------------------------------------------------------------------------------------------------------------------

class MongoDBCache(BaseCache):

    def __init__(self, location, params):
        BaseCache.__init__(self, params)
        self.location = location
        options = params.get('OPTIONS', {})
        self._host = options.get('HOST', 'localhost')
        self._port = options.get('PORT', 27017)
        self._database = options.get('DATABASE', 'django_cache')
        self._rshosts = options.get('RSHOSTS')
        self._rsname = options.get('RSNAME')
        self._user = options.get('USER', None)
        self._password = options.get('PASSWORD', None)
        self._server_selection_timeout_ms = options.get('SERVER_SELECTION_TIMEOUT_MS', 30000)
        self._socket_timeout_ms = options.get('SOCKET_TIMEOUT_MS', None)
        self._connect_timeout_ms = options.get('CONNECT_TIMEOUT_MS', 20000)
        self.compression_level = options.get('COMPRESSION_LEVEL', 0)
        self._tag_sets = options.get('TAG_SETS', None)
        self._read_preference = options.get("READ_PREFERENCE")
        self._collection = location
        self.log = logging.getLogger(__name__)

#-----------------------------------------------------------------------------------------------------------------------

    def add(self, key, value, timeout=None, version=None):
        key = self.make_key(key, version)
        self.validate_key(key)
        self._base_set('add', key, value, timeout)

#-----------------------------------------------------------------------------------------------------------------------

    def set(self, key, value, timeout=None, version=None):
        key = self.make_key(key, version)
        self.validate_key(key)
        self._base_set('set', key, value, timeout)

#-----------------------------------------------------------------------------------------------------------------------

    def _base_set(self, mode, key, value, timeout=None):
        no_safe = False
        if pymongo.version_tuple[0] >= 3:
            no_safe = True
        if not timeout:
            timeout = self.default_timeout
        now = datetime.utcnow()
        expires = now + timedelta(seconds=timeout)
        coll = self._get_collection()
        encoded = self._encode(value)
        document_size = len(encoded)
        count = coll.count()
        if count > self._max_entries:
            self._cull()
        data = coll.find_one({'_id': key})
        if data and (mode == 'set' or
                (mode == 'add' and data['expires'] > now)):
            raw = data.get('data')
            if raw and raw == encoded:
                if no_safe:
                    coll.update({'_id': data['_id']}, {'$set': {'expires': expires}})
                else:
                    coll.update({'_id': data['_id']}, {'$set': {'expires': expires}}, safe=True)
                return
            else:
                self._delete([key] + data.get('chunks', []))
        if document_size <= MAX_SIZE:
            if no_safe:
                coll.insert({'_id': key, 'data': encoded, 'expires': expires})
            else:
                coll.insert({'_id': key, 'data': encoded, 'expires': expires}, safe=True)
        else:
            chunks = []
            for i in xrange(0, document_size, MAX_SIZE):
                chunk = encoded[i:i+MAX_SIZE]
                aux_key = self.make_key(chunk)
                if no_safe:
                    coll.insert({'_id': aux_key, 'data': chunk})
                else:
                    coll.insert({'_id': aux_key, 'data': chunk}, safe=True)
                chunks.append(aux_key)
            if no_safe:
                coll.insert({'_id': key, 'chunks': chunks, 'expires': expires})
            else:
                coll.insert({'_id': key, 'chunks': chunks, 'expires': expires}, safe=True)

#-----------------------------------------------------------------------------------------------------------------------

    def _decode(self, data):
        return pickle.loads(zlib.decompress(base64.decodestring(data)))

#-----------------------------------------------------------------------------------------------------------------------

    def _encode(self, data):
        return base64.encodestring(zlib.compress(pickle.dumps(data, pickle.HIGHEST_PROTOCOL), self.compression_level))

#-----------------------------------------------------------------------------------------------------------------------

    def get(self, key, default=None, version=None):
        coll = self._get_collection()
        key = self.make_key(key, version)
        self.validate_key(key)
        now = datetime.utcnow()
        data = coll.find_one({'_id': key})
        if not data:
            return default
        if data['expires'] < now:
            coll.remove(data['_id'])
            return default
        raw = data.get('data')
        if not raw:
            chunks = data.get('chunks')
            if chunks:
                raw = ''
                for chunk in chunks:
                    raw += coll.find_one({'_id': chunk})['data']
            else:
                return default
        return self._decode(raw)

#-----------------------------------------------------------------------------------------------------------------------

    def get_many(self, keys, version=None):
        coll = self._get_collection()
        now = datetime.utcnow()
        out = {}
        parsed_keys = {}
        to_remove = []
        for key in keys:
            pkey = self.make_key(key, version)
            self.validate_key(pkey)
            parsed_keys[pkey] = key
        data = coll.find({'_id': {'$in': parsed_keys.keys()}})
        for result in data:
            if result['expires'] < now:
                to_remove.append(result['_id'])
            else:
                raw = result.get('data')
                chunks = result.get('chunks')
                if chunks:
                    raw = ''
                    for chunk in chunks:
                        raw += coll.find_one({'_id': chunk})['data']
                out[parsed_keys[result['_id']]] = self._decode(raw)
        if to_remove:
            coll.remove({'_id': {'$in': to_remove}})
        return out

#-----------------------------------------------------------------------------------------------------------------------

    def delete(self, key, version=None):
        key = self.make_key(key, version)
        self.validate_key(key)
        coll = self._get_collection()
        data = coll.find_one({'_id': key})
        if data:
            self._delete([key] + data.get('chunks', []))

#-------------------------------------------------------------------------------------------------------------

    def _delete(self, ids_to_remove):
        coll = self._get_collection()
        coll.remove({'_id': {'$in':ids_to_remove}})

#-----------------------------------------------------------------------------------------------------------------------

    def has_key(self, key, version=None):
        coll = self._get_collection()
        key = self.make_key(key, version)
        self.validate_key(key)
        data = coll.find_one({'_id': key, 'expires': {'$gt': datetime.utcnow()}})
        return data is not None

#-----------------------------------------------------------------------------------------------------------------------

    def clear(self):
        coll = self._get_collection()
        coll.remove({})

#-----------------------------------------------------------------------------------------------------------------------

    def _cull(self):
        if self._cull_frequency == 0:
            self.clear()
            return
        coll = self._get_collection()
        coll.remove({'expires': {'$lte': datetime.utcnow()}})
        #TODO: implement more agressive cull

#-----------------------------------------------------------------------------------------------------------------------

    def _get_collection(self):
        if not getattr(self, '_coll', None):
            self._initialize_collection()
        return self._coll

#-----------------------------------------------------------------------------------------------------------------------

    def _initialize_collection(self):
        try:
            from gevent import monkey
            monkey.patch_socket()
        except ImportError:
            pass

        if pymongo.version_tuple[0] < 3:

            if self._rsname:
                self.connection = pymongo.MongoReplicaSetClient(self._rshosts, replicaSet=self._rsname,
                    read_preference=self._read_preference, socketTimeoutMS=self._socket_timeout_ms,
                    connectTimeoutMS=self._connect_timeout_ms, tag_sets=self._tag_sets)
            else:
                self.connection = pymongo.Connection(self._host, self._port)

        else:
            self.connection = pymongo.MongoClient(connect=False, host=self._host, replicaset=self._rsname,
                sockettimeoutms=self._socket_timeout_ms, connecttimeoutms=self._connect_timeout_ms,
                serverSelectionTimeoutMS=self._server_selection_timeout_ms,read_preference=self._read_preference)

        self._db = self.connection[self._database]
        if self._user and self._password:
            self._db.authenticate(self._user, self._password)
        if pymongo.version_tuple[0] < 3:
            self._coll= self._db[self._collection]
        else:
            self._coll = self._db.get_collection(self._collection)
            if not self._coll:
                self._coll= self._db.create_collection(self._collection, storageEngine={'wiredTiger':{'configString':'block_compressor=none'}})

#-----------------------------------------------------------------------------------------------------------------------