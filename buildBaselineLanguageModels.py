import argparse
import os

SMOOTHING_ALPHA = .99
# to avoid OOV problems and zero probabilities, we will smooth the language models of each collection/cluster
# with a mixing variable alpha  weight of a term will be ALPHA(RTF)+(1-ALPHA)(CTF) where RTF is either a cluster or
# a resource and CTF is the collection frequency

# on second thought, this will end up requiring us to keep a value for every feature in the term space
# and ruin the sparse vector representation optimization.  With this in mind, we will instead assume
# all OOV terms have a frequency of 1, and add 1 to all term frequencies, implementing laplace smoothing.

def main(indexLocation,modelsLocation):
    models = open(modelsLocation,"w")
    docVecs = [indexLocation+f for f in os.listdir(indexLocation) if f.endswith(".vec#")]

    for i,d in enumerate(docVecs):

        aggregateVector = []
        docNum = d[46:49]
        print "computing model %s"%docNum
        for l in open(d).readlines():
            k,v = l.split(":")
            vec = eval(v)
            aggregateVector = addVecs(vec,aggregateVector)

        #laplace smoothing
        aggregateVector = [[t_id,tf+1] for [t_id,tf] in aggregateVector]

        print "writing model %s"%docNum
        models.write("%s:%s\n"%(docNum,str(aggregateVector)))
        models.flush()

def test():
    vec1 = [[1,1],[2,2],[5,5],[10,10]]
    vec2 = [[2,2],[3,3],[5,5],[11,11]]
    print(addVecs(vec1,vec2))


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
    parser.add_argument('modelsLocation',type=str)
    parser.add_argument('-v',action='store_true')

    
    args = parser.parse_args()

    if args.v:
        logging.getLogger().setLevel(logging.INFO)


    main(args.indexLocation,args.modelsLocation)


