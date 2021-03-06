import argparse
import os
from multiprocessing import Pool,current_process
from functools import partial
import gc

#SMOOTHING_ALPHA = .99
PROCESSES = 5 
INNER_PROCESSES = 4
# to avoid OOV problems and zero probabilities, we will smooth the language models of each collection/cluster
# with a mixing variable alpha  weight of a term will be ALPHA(RTF)+(1-ALPHA)(CTF) where RTF is either a cluster or
# a resource and CTF is the collection frequency

# on second thought, this will end up requiring us to keep a value for every feature in the term space
# and ruin the sparse vector representation optimization.  With this in mind, we will instead assume
# all OOV terms have a frequency of 1, and add 1 to all term frequencies, implementing laplace smoothing.

def main(indexLocation,clusterLocation,modelsLocation):
    """ Multithreaded language model computation """
    process_pool = Pool(PROCESSES)
    clusters = open(clusterLocation,"r")
    write_models = open(modelsLocation,"w")
    docVecs = sorted([f for f in os.listdir(indexLocation) if f.endswith(".vec#")])
    
    partial_computeModels = partial(computeModel, docVecs=docVecs, indexLocation=indexLocation)

    print "computing models"
    c = clusters.readlines()

    group = []
    models = []

    # assign one cluster computation per process, cuts down on memory consumption
    # write models to file before computing next batch
    for i,line in enumerate(c):
        group.append(line)
        if i%PROCESSES == PROCESSES - 1:
            models = []
            gc.collect()
            models =  process_pool.map(partial_computeModels,group)
            for j,m in enumerate(models):
                model_num = i - PROCESSES + 1 + j
                print "writing model %d"%model_num
                write_models.write(" %d:%s\n"%(model_num,str(m)))
                write_models.flush()
                
            group = []
            

    # write remaining models
    for j,m in enumerate(models):
        model_num = j + len(c)-len(models)
        print "writing model %d"%model_num
        write_models.write("%d:%s\n"%(model_num,str(m)))


def computeModel(c,docVecs,indexLocation):
    """given a cluster of documents, and the location of the corresponding documetn vectors, compute the language model """
        

        aggregateVector = []
        global process_pool
        k,docs = c.split(":")
        docs = eval(docs)
        docVecsIndex = 0
        currentOpenVecFile = 0
        docsIndex = 0
        vec_num = docVecs[docVecsIndex][18:21]
        vecFile = open(indexLocation+"/"+docVecs[docVecsIndex],"r")
        final_model = []

        print "building model"
        print "clust size: %d"%len(docs)
        for i,d in enumerate(docs):
            doc_num = d[:3]

            while(vec_num != doc_num):
                docVecsIndex +=1
                vec_num = docVecs[docVecsIndex][18:21]

            # make sure current open file corresponds to current docVecsIndex
            if(currentOpenVecFile != docVecsIndex):
                vecFile.close()
                vecFile = open(indexLocation+"/"+docVecs[docVecsIndex],"r")

            # find the line in the file with the right vector
            dVecNum,vec = vecFile.readline().split(":")
            while(dVecNum != d):
                l = vecFile.readline()
                try:
                    dVecNum,vec = l.split(":")

                except Exception as e:
                    print e
                    print l
                    print doc_num
                    continue

            try:
                aggregateVector.append(eval(vec))
            except Exception as e:
                    print e
                    print vec
                    print doc_num
                    print dVecNum
                    continue

            # confirm that we're making progress
            if i%500 ==0:
                print i

            # sum subsets at intervals of 2000
            if i %2000 == 0 and i != 0:
                print "Summing subset"

                sums = sum_groups(aggregateVector)
                aggregateVector = []
                vec_groups = []
                
                
                print "merging sums with final model"
                final_model = addVecs(sums,final_model)
                sums = []
                gc.collect()
                print "summed"


            
            

        docs = []
        print "collecting garbage"
        gc.collect()
        print "garbage collected"

        # sum remaining vectors
        sums = sum_groups(aggregateVector)
        vec_groups = []
        aggregateVector = []
        gc.collect()
        final_model = addVecs(sums,final_model)

        print "Completed sums calculations"
        return final_model


def addVecs(vec1,vec2):
    """ adds vec1 to vec2 """
    new_vec = []
    vec1_i=0
    vec2_j=0
    while(vec1_i<len(vec1) and vec2_j<len(vec2)):
        #print vec[vec_i][0],mean_vec[mean_vec_j][0]
        if(vec1[vec1_i][0]==vec2[vec2_j][0]):
            #print vec1[vec1_i]
            #print vec2[vec2_j]
            new_sum = vec1[vec1_i][1] + vec2[vec2_j][1] 
            new_vec.append([vec2[vec2_j][0],new_sum])
            vec1_i+=1
            vec2_j+=1
        elif vec1[vec1_i][0]>vec2[vec2_j][0]:
            new_vec.append(vec2[vec2_j])
            vec2_j+=1
        else:
            new_vec.append(vec1[vec1_i])
            vec1_i+=1

    
    # append remaining if stepped out of mean_vec bounds
    while(vec1_i<len(vec1)):
        new_vec.append(vec1[vec1_i])
        vec1_i += 1

    while(vec2_j<len(vec2)):
        new_vec.append(vec2[vec2_j])
        vec2_j += 1

    return new_vec

def sum_groups(vec_group):
    """" takes an array of vectors and returns the sum of the vectors """"
    v1 = []
    num_groups = len(vec_group)
    for i,v2 in enumerate(vec_group):
        if i%1000 == 0:
            print "%d of %d"%(i,num_groups)
        #print len(v2)
        if v2 == [[]]:
            continue
        v1 = addVecs(v1,v2)

    return v1


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='perform k-means clustering')
    parser.add_argument('indexLocation',type=str)
    parser.add_argument('clustersLocation',type=str)
    parser.add_argument('modelsLocation',type=str)
    parser.add_argument('-v',action='store_true')

    
    args = parser.parse_args()

    if args.v:
        logging.getLogger().setLevel(logging.INFO)


    main(args.indexLocation,args.clustersLocation,args.modelsLocation)
