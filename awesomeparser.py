# awesomeparser.py  
# By Ryan Eshleman
# 9/14/14
#
# This class does the parsing for the bsbmodified
# algorithm.  Includes maintaining the dictionary of term:term_id mappings

from nltk.stem.porter import *
import re,string
from collections import OrderedDict
from math import log


class AwesomeParser():
    def __init__(self):
        self.dictionary = OrderedDict()
        self.stemmer = PorterStemmer()
        self.doc_id = -1
        self.regex = re.compile('[%s]' % re.escape(string.punctuation))

    def parse(self,lines):
        """ Method takes an array of strings, preprocesses them, converts to
            <term_id,doc_id> stream   Also, maintains the dictionary of term:term_id mappings"""
        self.doc_id += 1
        stemmed = []
        out_stream = []
        
        # preprocessing:  tokenize each line, and send to clean_word() for 
        # cleansing
        for l in lines:
            stemmed += [self.clean_word(w) for w in l.split()]

        # map term to term_id with dictionary, adding dictionary entries as needed
        # also maintains count of each word.
        for w in stemmed:
            if not self.dictionary.has_key(w):
                t_id = len(self.dictionary)
                self.dictionary.update({w:[t_id,[self.doc_id]]})
            else:
                t_id = self.dictionary[w][0]
                self.dictionary[w][1] += [self.doc_id]
            
            out_stream.append([t_id,self.doc_id])

        return out_stream    

    

    def buildDictGetTIDList(self,text):
        """ takes a stream of text, processes it wrt the dictionary, returns
        a list of TIDs"""
        self.doc_id += 1
        out = []

        stemmed = [self.clean_word(w) for w in text.split()]
        for w in stemmed:
            if not self.dictionary.has_key(w):
                t_id = len(self.dictionary)
                self.dictionary.update({w:[t_id,[self.doc_id]]})
                out.append(t_id)
            else:
                t_id = self.dictionary[w][0]
                self.dictionary[w][1] += [self.doc_id]
                out.append(t_id)

        return out

    def clean_dictionary(self):
        """ duplicate doc ids are stored in the dictionary.
            count unique doc ids to get the document frequency, update dictionary """
        for item,[term_id,doc_ids] in self.dictionary.iteritems():
            df = len(set(doc_ids))
            self.dictionary[item] = [term_id,df]

    def clean_word(self,word):
        """ this method does 3 preprocessing steps:
            1.  gets rid of all punctiation as defined in the python string library
                punctionarion includes: '!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~'
            2.  case folding, (everything in lowercase)
            3.  Stemming using the Porter Stemmer included in the
                Python Natural Language Toolkit (NLTK) """

        w = self.regex.sub('',word)
        return self.stemmer.stem(w.lower())




