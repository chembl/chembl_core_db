__author__ = 'mnowotka'

import re
import collections

#-----------------------------------------------------------------------------------------------------------------------

chemblIDRegex = re.compile(r"CHEMBL\d+$")
smilesRegex = re.compile(r'^([^J][.0-9BCGOHMNSEPRIFTLUA@+\-\[\]\(\)\\\/%=#$]+)$')
numbersRegex = re.compile(r'(\d|%\d+)')
standardInchiKeyRegex = re.compile(r'^([0-9A-Z\-]+)$')
uniprotRegex = re.compile(r"[A-Z][0-9][A-Z0-9]{3}[0-9]((-([0-9]+)|:PRO_[0-9]{10}))?")
refseqRegex = re.compile(r"(NC|AC|NG|NT|NW|NZ|NM|NR|XM|XR|NP|AP|XP|YP|ZP)_[0-9]+")

#-----------------------------------------------------------------------------------------------------------------------

def isBalanced(strInput):
    if strInput:
        brackets = [ ('(',')'), ('[',']'), ('{','}')]
        kStart = 0
        kEnd = 1
        stack = []
        for char in strInput:
            for bracketPair in brackets:
                if char == bracketPair[kStart]:
                    stack.append(char)
                elif char == bracketPair[kEnd] and len(stack) > 0 and stack.pop() != bracketPair[kStart]:
                    return False
        if len(stack) == 0:
            return True
    return False

#-----------------------------------------------------------------------------------------------------------------------

def validateSmiles(smiles):
    if not smilesRegex.match(smiles.upper()):
        return False
    numbers = numbersRegex.findall(smiles)
    if not all((x % 2) == 0 for x in collections.Counter(numbers).values()):
        return False
    if not isBalanced(smiles):
        return False
    return True

#-----------------------------------------------------------------------------------------------------------------------

def validateChemblId(chemblId):
    return chemblIDRegex.match(chemblId)

#-----------------------------------------------------------------------------------------------------------------------

def validateStandardInchiKey(key):
    return len(key) == 27 and key[14] == '-' and key[25] == '-' and standardInchiKeyRegex.match(key)

#-----------------------------------------------------------------------------------------------------------------------

def validateUniprot(uniprot):
    return uniprotRegex.match(uniprot) and len(uniprot) == 6

#-----------------------------------------------------------------------------------------------------------------------

def validateRefseq(refseq):
    return refseqRegex.match(refseq)

#-----------------------------------------------------------------------------------------------------------------------