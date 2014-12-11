import argparse
import os
import re,string
from nltk.stem.porter import *
import string
import math

def main(modelsLocation,dictionaryLocation,queriesFile,resultslocation,baseline=False):
    if baseline:
        print "baseline"

    print "loading dict"
    dictionary = load_dict(dictionaryLocation)
    print "dict loaded"
    queries = open(queriesFile,"r")
    out = open(resultslocation,"w")

    print "loading models"
    resources,models,modelSizes = loadModels(modelsLocation)
    if not baseline:
        cluster_res_distributions = getClusterDistributions(modelsLocation)
    #print len(cluster_res_distributions)
    print "models loaded"


    stemmer = PorterStemmer()
    regex = re.compile('[%s]' % re.escape(string.punctuation))

    for q in queries.readlines():
        num,query = q.split("\t")

        # stem and pre-process query list
        q_as_t_id = [stemmer.stem(regex.sub('',word.lower())).replace("\n","") for word in query.split(" ")]

        # transofrm query into t_id list
        # if term not in dictionary, drop it
        q_as_t_id = [dictionary[w][0] for w in q_as_t_id if dictionary.has_key(w)]

        print "getting rankings"
        model_rankings = getRankings(q_as_t_id,models,modelSizes)
        #print rankings

        # if we are looking at the baseline, then the models map directly to resources
        print "writing scores"
        if baseline:
            for i,r in enumerate(model_rankings):
                print "%s Q0 FW14-e%s %d %d ryan"%(num,resources[r],i,149-i)
                out.write("%s Q0 FW14-e%s %d %d ryan\n"%(num,resources[r],i,149-i))
        else:
        # if we are looking at topic models, then the models correspond to
        # distirbutions of resources, so we have to do some further processing
            resource_rankings = []
            for r in model_rankings:
                cluster_dist = cluster_res_distributions[resources[r]]
                for res in cluster_dist:
                    if not res in resource_rankings:
			print "%s Q0 FW14-e%s %d %d ryan"%(num,res,99,99)
                        resource_rankings.append(res)

            for i,r in enumerate(resource_rankings):
                out.write("%s Q0 FW14-e%s %d %d ryan\n"%(num,r,i,149-i))


        
        

def getClusterDistributions(modelsLocation):
    """ compute the resource distribution within each cluster """

    clusters = open(modelsLocation.replace(".model",".clust"),"r")
    cluster_resource_distributions = {}
    
    for c in clusters.readlines():
        doc_counts = {}
        c_num,docs = c.split(":")
        docs = eval(docs)
        for d in docs:
            #print d
            doc_num = d[:3]
            #print doc_num
            if doc_counts.has_key(doc_num):
                doc_counts[doc_num] += 1
            else:
                doc_counts[doc_num] = 1

        rankings = [d_num for [d_num,count] in sorted(doc_counts.iteritems(),key=lambda x:x[1],reverse=True)]
        cluster_resource_distributions[c_num] = rankings

    return cluster_resource_distributions





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

def getRankings(q_as_t_id,models,sizes):
    sums = []
    for i,m in enumerate(models):
        if sizes[i]==0:
            print "empty model, uhoh: %d"%i
            sums.append(-10000000000)
            continue
        sums.append(getSumLogLikelihood(q_as_t_id,m,sizes[i]))

    #print sums

    rankings =  sorted(range(len(sums)), key=lambda k: sums[k],reverse=True)
    return rankings


def getSumLogLikelihood(q_as_t_id,m,size):

    tfs = [tf for [t_id,tf] in m if t_id in q_as_t_id]

    #print tfs

    #print [[tf,math.log(tf/float(size))] for tf in tfs]
    s = sum([math.log(tf/float(size)) for tf in tfs])

    # account for oov words, laplace smoothing
    model_t_ids = [mt_id for [mt_id,tf] in m]
    for tf in q_as_t_id:
        if tf not in model_t_ids:
            s += math.log(1/float(size))

    return s




        


def loadModels(modelsLocation):
    """ load language models into memory """
    mFile = open(modelsLocation,"r")
    resources = []
    models = []
    modelSizes = []
    for i,m in enumerate(mFile):
        resource,model = m.split(":")
        resource = resource.replace(" ","")
        resources.append(resource)
        model = eval(model)
        models.append(model)
        modelSizes.append(sum([tf for [tid,tf] in model]))

    return resources,models,modelSizes



if __name__ == '__main__':


    parser = argparse.ArgumentParser(description='perform query on language models')
    parser.add_argument('models',type=str)
    parser.add_argument('dictionary',type=str)
    parser.add_argument('queries',type=str)
    parser.add_argument('resultsLocation',type=str)
    parser.add_argument('-v',action='store_true')
    parser.add_argument('-b',action='store_true')

    
    args = parser.parse_args()

    if args.v:
        logging.getLogger().setLevel(logging.INFO)


    main(args.models,args.dictionary,args.queries,args.resultsLocation,args.b)


