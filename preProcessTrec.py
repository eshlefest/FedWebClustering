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


def main(dataLocation,indexLocation,sampleSize):

    ledger = open(indexLocation +"/ledger.txt",'r+a')
    alreadyProcessed = [l.strip() for l in ledger.readlines()]


    tars = [t for t in os.listdir(dataLocation) if t not in alreadyProcessed and t.find("doc") != -1]

    for tar in tars:
        #if tar.find("doc") == -1:
        #    continue
        parser = AwesomeParser()
        print tar, sampleSize
        count=1
        with tarfile.open(dataLocation +"/"+ tar) as tf:
            out = open(indexLocation +"/"+tar.replace(".tgz","#%d.vec"%count),'w')
            names = tf.getnames()
            num_files = len(names)
            skipped = 0
            for i,entry in enumerate(names):
                if not entry.endswith('.html'):
                    #print entry,i
                    skipped +=1
                    continue
                i = i - skipped
                #print i
                #if i % 5000 == 0:
                #    write_dictionary(parser,indexLocation,tar.replace("/","#").replace(".tgz","#%d.dict"%count))
                #    count += 1
                #    out.flush()
                #    out.close()
                #    out = open(indexLocation +"/"+tar.replace(".tgz","#%d.vec"%count),'w')
                #    parser.dictionary = {}
                #    parser = AwesomeParser()
                #    print "partition %d"%count
                 
                  
                if (float(i) / num_files) * 100 > sampleSize:
                    break

                if(i%100 == 0):
                    print "%d of %d (skipped %d)"%(i,num_files,skipped)
                    out.flush()
                    #print float(i) / num_files
                file_obj = tf.extractfile(entry)


                # sometimes we run into unicode exceptions
                # instead of doing the responsible thing and
                # figuring out the problem, we skip the doc.
                # we are sampling after all...
                try:
                    text = getTextFromHTMLFile(file_obj)
                    #print text
                except Exception as e:
                    print "Exception Caught"

                    print e
                    #exit()
                    continue
                #print text
                TIDList = parser.buildDictGetTIDList(text)
                TIDList = processTIDList(TIDList)
                out.write("%d:"%i + str(TIDList) + "\n")

                
        write_dictionary(parser,indexLocation,tar.replace("/","#").replace(".tgz","#%d.dict"%count))
        ledger.write(tar+"\n")
        ledger.flush()


    #print len(parser.dictionary)

def getTextFromHTMLFile(file_obj):
    t = file_obj.read()#.decode('ascii', 'ignore')
    #print t
    cleanedText = lxml.html.clean.clean_html(t)
    #cleanedText
    #print cleanedText
    #html.decode('utf-8', 'ignore')
    soup = BeautifulSoup(cleanedText.decode('ascii','ignore'))
    text = soup.get_text()
    return text

def processTIDList(TIDList):
    return [[w,TIDList.count(w)] for w in sorted(set(TIDList))]

    
def write_dictionary(parser,destination,name):
    """ writes the dictionary to disk, after calling its 'clean' function """

    write = open(destination+"/dictionaries/"+name,"w")

    parser.clean_dictionary()

    d = parser.dictionary
    size = len(d)

    out = ""
    for j,i in enumerate(d.iteritems()):
        print "%d of %d"%(j,size)
        if(j%20000 == 0):
            write.write(out.encode('ascii','ignore'))
            out = ""
        out += i[0] + ": " + str(i[1]) + "\n"
        
    write.write(out.encode('ascii','ignore'))
    write.close()    

 

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='preprocess text data into vectors')
    parser.add_argument('dataLocation',type=str)
    parser.add_argument('indexLocation',type=str)
    parser.add_argument('-s',type=int)
    parser.add_argument('-v',action='store_true')
    sampleSize = 100
    
    args = parser.parse_args()

    if args.v:
        logging.getLogger().setLevel(logging.INFO)

    if args.s:
        sampleSize = args.s

    main(args.dataLocation,args.indexLocation,sampleSize)



