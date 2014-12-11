import argparse
import os
import random
import math
import time
from multiprocessing import Pool
from functools import partial

CONVERGENCE_THRESHOLD = .05
CENTROID_TRIM_TOP_N = 2500
ITERATION_LIMIT = 10
SAMPLE_DIVISOR = 2


def main(indexLocation,k,outfile):
    """ Performs K-Means clustering, with a maximum number of iterations set by ITERATION_LIMIT, centroid trimming 
        determined by CENTROID_TRIM_TOP_N and sample rate determined by SAMPLE_DIVISOR.  As SAMPLE_DIVISOR value of 2
        indicates a sample rate of %50 """
    start_time = time.time()

    out = open(outfile+".txt","w")
    clusters_out = open(outfile+".clust","w")

    # get vecfile names from location
    vecFile = sorted([f for f in os.listdir(indexLocation) if f.endswith(".vec#tfidf")])
    docs = []
    vecs = []
    centroid_update_sim = 0

    process_pool = Pool(8)
    #centroid_assignments =
    #global centroids

    # randomly assign centroids
    centroids = initializeCentroids(indexLocation,vecFile,k)
    
    
    # write some logging info to file
    out.write("k=%d Convergence: %f TopN: %d\n"%(k,CONVERGENCE_THRESHOLD,CENTROID_TRIM_TOP_N))
    outstr = "iteration"
    for i in range(k):
        outstr += ",c%d_size,c%d_sim"%(i,i)

    out.write(outstr+",avg_sim\n")

    
    iteration = 1

    print "generating random sample indexes"
    sample_indexes = getSampleIndexes(indexLocation,vecFile,SAMPLE_DIVISOR)

    print "here we go"

    # main k-means loop
    # centroid_update_sim is compute at the end of each loop, and compared here
    # the loop ends when new and old centroids are sufficiently similar
    while(1 - centroid_update_sim > CONVERGENCE_THRESHOLD):
        new_centroids = [[] for b in range(k)]
        new_centroids_count = [0 for a in range(k)]
        
        # for resource in indexLocation
        for i,f in enumerate(vecFile):

            print "loading vec: %s"%f
            vecs = []
            openVecFile = open(indexLocation+f,'r')
            
            # retreive sample
            lines = [line for vecnum,line in enumerate(openVecFile.readlines()) if vecnum in sample_indexes[i]]
            openVecFile.close()

            for line in lines:
                num,v = line.split(":")
                #print num
                #print v
                vecs.append(eval(v))

            lines = ""

            #print len(vecs)
            #print vecs

            # needed for multiple parameter passing and multiprocessing
            partial_findNearestCentroid = partial(findNearestCentroid,centroids=centroids)

            
            # send centroid assignment computations out to process pool
            print "finding nearest centroids"
            nearest_centroids = process_pool.map(partial_findNearestCentroid,vecs)

            print "found nearest centroids!"
            #print nearest_centroids

            # group vectors by centroid assignment
            vec_groups = []
            for i in range(len(centroids)):
                vec_groups.append([v for j,v in enumerate(vecs) if nearest_centroids[j] == i])
                

            # sum all vectors assigned to each centroid, multiprocessed
            print "summing groups"
            sums = process_pool.map(sum_groups, vec_groups)
            print "groups summed!"
            #print sums

            # addvecs probably not necessary hear, leftover from previous iterations
            for i,s in enumerate(sums):
                new_centroids[i] = addVecs(s,new_centroids[i])

                

            for i in range(len(new_centroids_count)):
                new_centroids_count[i] += nearest_centroids.count(i)

            trim_centroids(new_centroids)
            out.flush()
            


        # at this point, we have assigned all vectors to centroids, and have summed all vectors corresponding
        # to the centroids.  in the next step we will compute the new centroid values and normalize them.
        # we will also compute the similarity between the new centroids and the old ones.
            
        outstring = "%d"%iteration
        #find mean
        centroid_update_sim= 0
        num_empty = 0
        trim_centroids(new_centroids)
        for i,bigVec in enumerate(new_centroids):
            
            newCent = normalize([[t_id,v/float(new_centroids_count[i])] for [t_id,v] in bigVec])
            if len(newCent) == 0:
                num_empty += 1
            
            sim = computeSimilarity(newCent,centroids[i])
            centroids[i] = newCent
            print "sim between old and new centroid %d(%d):   %f"%(i,len(newCent),sim)
            centroid_update_sim += sim
            outstring += ",%d,%f"%(len(newCent),sim)


        centroid_update_sim /=float(len(centroids) - num_empty)
        print "average sim: %f"%(centroid_update_sim)
        out.write(outstring +","+str(centroid_update_sim)+"\n")
        iteration +=1
        trim_centroids(centroids)

        if iteration > ITERATION_LIMIT:
            break

    out.write("ElapsedTime: %f"%(time.time() - start_time))

    write_clusters(centroids,clusters_out,vecFile,indexLocation,process_pool)
    out.close()


def sum_groups(vec_group):
    "takes an array of vectors, and sums them "
    v1 = []
    for v2 in vec_group:
        #print len(v2)
        if v2 == [[]]:
            continue
        v1 = addVecs(v1,v2)

    return v1

def write_clusters(centroids,clusters_out,vecFile,indexLocation,process_pool):
    """ given an array of centroids, write the clusters to file   """

    clusters = [[] for a in range(len(centroids))]
    print "writing clusters"

    for i,f in enumerate(vecFile):
        print "loading vec: %s"%f
        vecs = []
        doc_nums = []
        for line in open(indexLocation+f,'r').readlines():
            doc_num,v = line.split(":")
            vecs.append(eval(v))
            doc_nums.append(doc_num)

        #print len(vecs)
        partial_findNearestCentroid = partial(findNearestCentroid,centroids=centroids)

        #print centroids
        print "finding nearest centroids"
        nearest_centroids = process_pool.map(partial_findNearestCentroid,vecs)
        print "found"

        for j,vec in enumerate(vecs):
            
            if vec == [[]]:
                continue
            nearest = nearest_centroids[j]
            clusters[nearest].append(doc_nums[j])
            if j %500 == 0:
                print j

        

    for k,c in enumerate(clusters):
        clusters_out.write("%d:%s\n"%(k,str(c)))

        
    clusters_out.close()


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

def findNearestCentroid(vec,centroids):
    """ computes similarity between vec and all centroids
        returns centroid with highest with highest similarity """
    max_sim = 0#computeSimilarity(vec,centroids[0])
    max_sim_centroid = 0
    current_sim = 0
    for k,centroid in enumerate(centroids):
        try:
            current_sim = computeSimilarity(vec,centroid)
            #print centroid
        except Exception as e:
            print "Exception: "+ str(e)
            print vec
            continue
        #print current_sim, max_sim
        if current_sim > max_sim:
            max_sim = current_sim
            max_sim_centroid = k

    return max_sim_centroid



def initializeCentroids(indexLocation,vecFile,k):
    """ choose K random documents from indexLocation, initialize them as centroids """
    centroids = []

    for i in range(k):    
        f = open(indexLocation+"/"+vecFile[random.randint(0,len(vecFile)-1)])



        lines = f.readlines()

        line = lines[random.randint(0,len(lines)-1)]
        l,vec = line.split(":")

        centroids.append(eval(vec))

    return centroids


def trim_centroids(centroids):
    """ leave the top N values, lose the rest """
    for i,c in enumerate(centroids):
        if c == [[]]:
            continue
        weights = sorted(c,key=lambda x:x[1])
        cutoff = len(c) - CENTROID_TRIM_TOP_N if len(c) > CENTROID_TRIM_TOP_N else 0
        centroids[i] = sorted(weights[cutoff:],key=lambda x:x[0])



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
            #print vec1[i][0]
            #print vec2[j][0]
            #print dot_product
            dot_product += (vec1[i][1]*vec2[j][1])
            i+=1
            j+=1
        elif vec1[i][0]>vec2[j][0]:
            j+=1
        else:
            i+=1

    return dot_product

def normalize(vec):
    """ normalizes the vector """
    length = math.sqrt(sum([(v*v) for [t_id,v] in vec]))
    return [[t_id,v/length] for [t_id,v] in vec]

def getSampleIndexes(indexLocation,vecFile,SAMPLE_DIVISOR):
    rand_sample = []
    for _file in vecFile:
        num_vecs = sum(1 for line in open(indexLocation+"/"+_file))
        rand_sample.append(sorted(random.sample(range(num_vecs),num_vecs/SAMPLE_DIVISOR)))

    return rand_sample
    



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='perform k-means clustering in low memory environment')
    parser.add_argument('indexLocation',type=str)
    parser.add_argument('k',type=int)
    parser.add_argument('outfile',type=str)
    parser.add_argument('-v',action='store_true')

    
    args = parser.parse_args()

    if args.v:
        logging.getLogger().setLevel(logging.INFO)


    main(args.indexLocation, args.k,args.outfile)


