import argparse
import os

SMOOTHING_ALPHA = .99
# to avoid OOV problems and zero probabilities, we will smooth the language models of each collection/cluster
# with a mixing variable alpha  weight of a term will be ALPHA(RTF)+(1-ALPHA)(CTF) where RTF is either a cluster or
# a resource and CTF is the collection frequency

# on second thought, this will end up requiring us to keep a value for every feature in the term space
# and ruin the sparse vector representation optimization.  With this in mind, we will instead assume
# all OOV terms have a frequency of 1, and add 1 to all term frequencies, implementing laplace smoothing.

def main(indexLocation,clusterLocation,modelsLocation):
    clusters = open(clusterLocation,"r")
    models = open(modelsLocation,"w")
    docVecs = [f for f in os.listdir(indexLocation) if f.endswith(".vec#")]

    for i,c in enumerate(clusters.readlines()):
        aggregateVector = []

        k,docs = c.split(":")
        docs = eval(docs)
        docVecsIndex = 0
        currentOpenVecFile = 0
        docsIndex = 0
        vec_num = docVecs[docVecsIndex][18:21]
        vecFile = open(indexLocation+"/"+docVecs[docVecsIndex],"r")

        print "building model: %d"%i
        print "clust size: %d"%len(docs)
        for i,d in enumerate(docs):
            doc_num = d[:3]

            while(vec_num != doc_num):
                docVecsIndex +=1
                vec_num = docVecs[docVecsIndex][18:21]

            # make sure current open file corresponds to current docVecsIndex
            if(currentOpenVecFile != docVecsIndex):
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


            if i %500 == 0:
                print i
            vec = eval(vec)
            if vec == [[]]:
                continue
            aggregateVector = addVecs(vec,aggregateVector)

        print "writing model %d"%i
        models.write("%d:%s\n"%(i,str(aggregateVector)))
        models.flush()


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