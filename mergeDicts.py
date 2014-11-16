# Ryan Eshleman  queryEngine.py  11-9-14
# 
# Takes a directory full of trecFedWeb tarballed data and transforms 
# each doc into a sparse vactor representation termId:tf
#

import argparse
import os
import ast,re,string
import math
import logging
import tarfile
import nltk
from bs4 import BeautifulSoup
import lxml.html.clean
from awesomeparser import AwesomeParser


def main(indexLocation):
    dictDirectory = indexLocation +"/dictionaries"
    masterDict = {}

    #dirs = os.listdir(dictDirectory)
    #for d in dirs:
    #    if d.endswith(".dict"):
    #        print d

    if "master.dict" not in os.listdir(dictDirectory):
        merge_dictionaries(dictDirectory,masterDict)
        write_dictionary(masterDict,indexLocation,"master.dict")
    else:
        masterDict = load_dict(dictDirectory+"/master.dict")

    print "Dictionary Loaded, updating vectors"

    update_doc_vecs(masterDict,indexLocation)




def merge_dictionaries(dictDirectory,masterDict):
    #dicts = [load_dict(dictDirectory +"/"+t) for t in os.listdir(dictDirectory) if t.endswith(".dict")]

    for dictionary in os.listdir(dictDirectory):
        if dictionary.endswith(".dict"):
            d = load_dict(dictDirectory +"/"+dictionary)
            print "merging: "+dictionary
            merge(masterDict,d)

        #di
    #for i,d in enumerate(dicts):
    #    print i
    #    merge(masterDict,d)

def update_doc_vecs(masterDict, indexLocation):
    doc_vecs_groups = [f for f in os.listdir(indexLocation) if f.endswith(".vec")]
    #print doc_vecs
    t_id_mapper = {}

    for group in doc_vecs_groups:
        group_id = group[-9:-6]
        print group_id
        t_id_mapping = {}
        v = open(indexLocation+"/"+group,'r')
        out = open(indexLocation+"/global_vecs/"+group+"#",'w')
        d = get_dictionary(group,indexLocation)
        #print group_id, d["comput"]

        # build global t_id mapping

        for key,[t_id,df] in d.iteritems():
            #print key,t_id,df

            if not masterDict.has_key(key):
                print "uh oh"
                exit()
            global_tid = masterDict[key][0]

            t_id_mapping[t_id] = global_tid
            # holds mappings from old_tid => new t_id


        #print t_id_mapping[1]    
        for line in v.readlines():
            doc_id,vec = line.split(":")
            doc_id = "%s_%s"%(group_id,doc_id)
            #vec = eval(vec)
            #print t_id_mapping[1]
            try:
                vecLine = [[t_id_mapping[tid],tf] for [tid,tf] in eval(vec)]
            except Exception as e:
                print doc_id
                vecLine = [[t_id_mapping[tid],tf] for [tid,tf] in eval(vec) if t_id_mapping.has_key(tid)]
                for v in eval(vec):
                    if not t_id_mapping.has_key(v[0]):
                        print "missing key: " + str(v)

                
            vecLine.sort(key=lambda x:x[0])
            out.write(doc_id+":"+str(vecLine)+"\n")




def get_dictionary(vec,indexLocation):
    dictDirectory = indexLocation +"/dictionaries"
    candidates = [d for d in os.listdir(dictDirectory) if d.find(".dict") != -1]
    #print 
    for c in candidates:

        if c.replace(".dict","") == vec.replace(".vec",""):
            print "Candidate Dict: " + c
            print "vec: " + vec
            return load_dict(dictDirectory+"/"+c)



def merge(masterDict,newDict):
    for key,[t_id,df] in newDict.iteritems():
        if not key in masterDict:
            masterDict[key] = [len(masterDict),df]
        else:
            masterDict[key][1]+= df


def load_dict(dictionary_location):
    """This function reads from the specified directory and inserts each line
       of the file into a hash map, this builds the dictionary in memory """
    dictionary_text = file(dictionary_location)
    dictionary = {}
    num_redundant = 0;

    for d in dictionary_text.readlines():
        split = d.split(":")
        tok = split[0]

        val = eval(split[1])
        if dictionary.has_key(tok):
            tok = tok +"num_redundant"+ str(num_redundant)
            num_redundant += 1
            


        dictionary[tok] = val

    #print dictionary
    return dictionary

def write_dictionary(dictionary,destination,name):
    """ writes the dictionary to disk, after calling its 'clean' function """

    write = open(destination+"/dictionaries/"+name,"w")

    out = ""
    for i in dictionary.iteritems():
        out += i[0] + ": " + str(i[1]) + "\n"
        
    write.write(out)
    write.close() 


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='merge dictionaries')
    parser.add_argument('indexLocation',type=str)
    parser.add_argument('-v',action='store_true')
    
    
    args = parser.parse_args()

    if args.v:
        logging.getLogger().setLevel(logging.INFO)

    main(args.indexLocation)



