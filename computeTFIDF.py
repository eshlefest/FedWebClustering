import os
import argparse
import math


def main(indexLocation,dictLocation):
    """This program computes tf-idf scores of each term, then normalizes
       by the vector length, producing a unit vector, writes it to disk"""

    num_docs = get_num_docs(indexLocation)
    print "num docs: %d"%num_docs
    
    print "Loading idf Dictionary"
    master_dict = load_idf_dict(dictLocation,num_docs)
    print "dict loaded"

    vecFiles = [f for f in os.listdir(indexLocation) if f.endswith(".vec#")]
    #print vecFiles

    for vecFile in vecFiles:
        print "updating: %s"%vecFile
        write = open(indexLocation+"/"+vecFile+"tfidf","w")
        out = ""
        for line in open(indexLocation+"/"+vecFile,'r'):
            doc,vec = line.split(":")
            out += doc + ":"
            vec = eval(vec)
            un_normalized_vec = [[t_id,tf*master_dict[tf]] for [t_id,tf] in vec]
            normalized_vec = normalize(un_normalized_vec)
            #print vec
            #print un_normalized_vec
            #print normalized_vec
            out += str(normalized_vec)
        
        write.write(out)




def normalize(vec):
    """ normalizes the vector """
    length = math.sqrt(sum([(v*v) for [t_id,v] in vec]))
    return [[t_id,round(v/length,4)] for [t_id,v] in vec]


def get_num_docs(indexLocation):
    vecFiles = [f for f in os.listdir(indexLocation) if f.endswith(".vec#")]

    lines = 0
    for vecFile in vecFiles:
        f = open(indexLocation+"/"+vecFile,"r")
        lines += len(f.readlines())

    return lines



def load_idf_dict(dictionary_location,numDocs):
    """Loads a dictionary mapping t_id to idf"""

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
            
        idf = math.log(numDocs/float(val[1]))

        dictionary[val[0]] = idf

    #print dictionary
    return dictionary



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='perform k-means clustering')
    parser.add_argument('indexLocation',type=str)
    parser.add_argument('dictLocation',type=str)
    parser.add_argument('-v',action='store_true')

    
    args = parser.parse_args()

    if args.v:
        logging.getLogger().setLevel(logging.INFO)


    main(args.indexLocation, args.dictLocation)