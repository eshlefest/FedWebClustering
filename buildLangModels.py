import argparse
import os

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

        for d in docs:
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

            vec = eval(vec)
            if vec == [[]]:
                continue
            aggregateVector = addVecs(vec,aggregateVector)

        print "writing model %d"%i
        models.write("%d:%s\n"%(i,str(aggregateVector)))


def addVecs(vec1,vec2):
    """ adds vec1 to vec2 """

    vec1_i=0
    vec2_j=0
    while(vec1_i<len(vec1) and vec2_j<len(vec2)):
        #print vec[vec_i][0],mean_vec[mean_vec_j][0]
        if(vec1[vec1_i][0]==vec2[vec2_j][0]):
            #print vec1[vec1_i]
            #print vec2[vec2_j]
            vec2[vec2_j][1] += vec1[vec1_i][1] 
            vec1_i+=1
            vec2_j+=1
        elif vec1[vec1_i][0]>vec1[vec2_j][0]:
            vec2_j+=1
        else:
            #print "append"
            vec2.append(vec1[vec1_i])
            vec1_i+=1
    
    # append remaining if stepped out of mean_vec bounds
    while(vec1_i<len(vec1)):
        vec2.append(vec1[vec1_i])
        vec1_i += 1

    return vec2




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