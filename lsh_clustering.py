import argparse
import os
import random
import math
import time
from multiprocessing import Pool
from functools import partial
import numpy

CONVERGENCE_THRESHOLD = .05
CENTROID_TRIM_TOP_N = 2500
ITERATION_LIMIT = 10
SAMPLE_DIVISOR = 2
PROCESSES = 4

DICT_SIZE = 7538504


def main(indexLocation,bits_per_signature,outfile):
    start_time = time.time()

    rand_planes = numpy.random.randn(bits_per_signature,DICT_SIZE)
    print len(rand_planes)
    partial_compute_fingerprint = partial(compute_fingerprint,hyperplanes=rand_planes)

    out = open(outfile+".txt","w")
    clusters_out = open(outfile+".clust","w")

    vecFile = sorted([f for f in os.listdir(indexLocation) if f.endswith(".vec#tfidf")])
    docs = []
    vecs = []
    centroid_update_sim = 0

    process_pool = Pool(PROCESSES)

    print "here we go"

    clusters = [[] for i in range(2**bits_per_signature)]
    
    for i,f in enumerate(vecFile):
            
        print "loading vec: %s"%f
        vecs = []
        nums = []
        openVecFile = open(indexLocation+f,'r')
        # retreive sample
        lines = openVecFile.readlines()
        openVecFile.close()

        for line in lines:
            num,v = line.split(":")
            #print num
            #print v
            nums.append(num)
            vecs.append(eval(v))

        lines = ""

        print len(vecs)
        #print vecs

        fingerprints = process_pool.map(partial_compute_fingerprint,vecs)
        
        for i,f in enumerate(fingerprints):
            clusters[f].append(nums[i])

    out.write("ElapsedTime: %f"%(time.time() - start_time))

    write_clusters(clusters,clusters_out)
    out.close()

def compute_fingerprint(vector,hyperplanes):
    res = 0
    for p in (hyperplanes):
        res = res << 1
        val = sparse_dot_product(vector, p)
        if val >= 0:
            res |= 1
    return res


def write_clusters(clusters,clusters_out):
    print "writing clusters"

    for i,c in enumerate(clusters):
        clusters_out.write("%d: %s\n"%(i,str(c)))

        
    clusters_out.close()

def sparse_dot_product(sparse_vec,hyperplane):
    dot = 0
    for i,v in sparse_vec:
        dot += v * hyperplane[i]

    return dot

def test_dot_product_function():
    vec_sparse = [[1,2],[2,3],[5,5],[6,5],[7,10]]
    vec_full = [0,2,3,0,0,5,5,10]
    rand = numpy.random.randn(8)

    a = sparse_dot_product(vec_sparse,rand)
    b = numpy.dot(rand,vec_full)

    print a,b







if __name__ == '__main__':
    test_dot_product_function()


    parser = argparse.ArgumentParser(description='perform lsh clustering')
    parser.add_argument('indexLocation',type=str)
    parser.add_argument('bits_per_signature',type=int)
    parser.add_argument('outfile',type=str)
    parser.add_argument('-v',action='store_true')

    
    args = parser.parse_args()

    if args.v:
        logging.getLogger().setLevel(logging.INFO)


    main(args.indexLocation, args.bits_per_signature,args.outfile)


