import argparse
import os
import random
import math

def main(indexLocation,k):
    #indices = open(indexLocation,"r")

    vecFile = [f for f in os.listdir(indexLocation) if f.endswith(".vec#tfidf")]
    docs = []
    vecs = []
    centroid_update_sim = 0
    UPDATE_THRESHOLD = .001
    #centroid_assignments =
    
    print "Loading Vectors"
    for i,f in enumerate(vecFile):
        print "loading vec: %s"%f
        for vec in open(indexLocation+f,'r').readlines():
            #print vec
            try:
                doc,v = vec.split(":")
            except Exception as e:
                print e
                print vec
                exit()

            docs.append(doc)
            vecs.append(eval(v))
        break

    print len(vecs)
        
        

    # initialize centroid vector
    centroids = centroids = initializeCentroids(indexLocation,vecFile,k)#[vecs[random.randint(0,len(vecs)-1)] for i in range(k)]
    centroid_assignments = [[] for i in range(k)]


    while(1 - centroid_update_sim > UPDATE_THRESHOLD):

        for i,vec in enumerate(vecs):
            max_sim = computeSimilarity(v,centroids[0])
            max_sim_centroid = 0
            current_sim = 0
            for k,centroid in enumerate(centroids):
                current_sim = computeSimilarity(vec,centroid)
                #print current_sim, max_sim
                if current_sim > max_sim:
                    max_sim = current_sim
                    max_sim_centroid = k

            centroid_assignments[max_sim_centroid].append(i)

    #for a in centroid_assignments:
        #print a
        centroid_update_sim= 0
    # update centroids
        for k in range(len(centroid_assignments)):
            print "computing mean for centroid %d"%k
            new_centroid = computeMean([vecs[i] for i in centroid_assignments[k]])
            c_sim = computeSimilarity(new_centroid,centroids[k])
            print c_sim
            centroid_update_sim += c_sim
            centroids[k] =new_centroid

        centroid_update_sim /=float(len(centroids))
        print "average sim: %f"%(centroid_update_sim)


def computeMean(vecs):
    mean_vec = []
    num_vecs = len(vecs)
    #print vecs
    
    
    for vec in vecs:
        vec_i=0
        mean_vec_j=0
        while(vec_i<len(vec) and mean_vec_j<len(mean_vec)):
            #print vec[vec_i][0],mean_vec[mean_vec_j][0]
            if(vec[vec_i][0]==mean_vec[mean_vec_j][0]):
                mean_vec[mean_vec_j][1] += vec[vec_i][1] 
                vec_i+=1
                mean_vec_j+=1
            elif vec[vec_i][0]>mean_vec[mean_vec_j][0]:
                mean_vec_j+=1
            else:
                #print "append"
                mean_vec.append(vec[vec_i])
                vec_i+=1
        
        # append remaining if stepped out of mean_vec bounds
        while(vec_i<len(vec)):
            mean_vec.append(vec[vec_i])
            vec_i += 1

    #print len(mean_vec)

    return normalize([[t_id,v/float(num_vecs)] for [t_id,v] in mean_vec])


def computeSimilarity(vec1,vec2):
    """compute cosine similarity between already normalized vectors """
    dot_product = 0
    #print len(vec1),len(vec2)

    i=0
    j=0
    while(i<len(vec1) and j<len(vec2)):
        if(vec1[i][0]==vec2[j][0]):
            #print "found similarity!"
            dot_product += (vec1[i][1]*vec2[j][1])
            i+=1
            j+=1
        elif vec1[i][0]>vec2[j][0]:
            j+=1
        else:
            i+=1

    return dot_product

def initializeCentroids(indexLocation,vecFile,k):
    centroids = []

    for i in range(k):    
        f = open(indexLocation+"/"+vecFile[random.randint(0,len(vecFile)-1)])
        lines = f.readlines()

        line = lines[random.randint(0,len(lines)-1)]
        l,vec = line.split(":")

        centroids.append(eval(vec))

    return centroids

def normalize(vec):
    """ normalizes the vector """
    length = math.sqrt(sum([(v*v) for [t_id,v] in vec]))
    return [[t_id,v/length] for [t_id,v] in vec]

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='perform k-means clustering')
    parser.add_argument('indexLocation',type=str)
    parser.add_argument('k',type=int)
    parser.add_argument('-v',action='store_true')

    
    args = parser.parse_args()

    if args.v:
        logging.getLogger().setLevel(logging.INFO)


    main(args.indexLocation, args.k)