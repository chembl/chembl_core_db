from django.test.client import MULTIPART_CONTENT
from tastypie.test import TestApiClient as TastypieTestApiClient
from tastypie.test import ResourceTestCase as TastypieResourceTestCase

class TestApiClient(TastypieTestApiClient):

    def post(self, uri, format='json', data=None, authentication=None, content_type=MULTIPART_CONTENT, **kwargs):

        accept = self.get_content_type(format)
        kwargs['HTTP_ACCEPT'] = accept

        if content_type==MULTIPART_CONTENT:
            contentType = content_type
        else:
            contentType = self.get_content_type(content_type)
        kwargs['content_type'] = contentType

        if data is not None:
            if content_type==MULTIPART_CONTENT:
                kwargs['data'] = data
            else:
                kwargs['data'] = self.serializer.serialize(data, format=contentType)

        if authentication is not None:
            kwargs['HTTP_AUTHORIZATION'] = authentication

        return self.client.post(uri, **kwargs)

class ResourceTestCase(TastypieResourceTestCase):

    def assertHttpOK(self, resp, url = None):
        """
        Ensures the response is returning a HTTP 200.
        """
        return self.assertEqual(resp.status_code, 200, "response code is not 200 but " + str(resp.status_code) + " for url" + str(url))

    def assertValidJSON(self, data, url = None):
        """
        Given the provided ``data`` as a string, ensures that it is valid JSON &
        can be loaded properly.
        """
        # Just try the load. If it throws an exception, the test case will fail.
        try:
            self.serializer.from_json(data)
        except:
            raise Exception("Can't serialise data for url" + url)

    def assertValidJSONResponse(self, resp, url = None):
        """
        Given a ``HttpResponse`` coming back from using the ``client``, assert that
        you get back:

        * An HTTP 200
        * The correct content-type (``application/json``)
        * The content is valid JSON
        """
        self.assertHttpOK(resp, url)
        self.assertTrue(resp['Content-Type'].startswith('application/json'), "content-Type is not application/json but " + resp['Content-Type'] + " for url " + str(url))
        self.assertValidJSON(resp.content, url)