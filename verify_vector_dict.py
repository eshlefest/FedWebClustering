import argparse

def main(indexLocation,vector):
    """ Utility file to make sure the preprocessing script is functioning correctly """

    dict_loc = indexLocation+"/dictionaries/"

    d = load_dict(dict_loc+vector)
    v = open(indexLocation+"/"+vector)

    t_ids = [t_id for key,[t_id,df] in d.iteritems() ]

    for line in v.readlines():
            doc_id,vec = line.split(":")
            vec = eval(vec)
            for[tid,df] in vec:
                print tid
                if tid not in t_ids:
                    print "not found:"
                    print tid
                    return



def load_dict(dictionary_location):
    """This function reads from the specified directory and inserts each line
       of the file into a hash map, this builds the dictionary in memory """
    dictionary_text = file(dictionary_location.replace(".vec",".dict"))
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





if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='merge dictionaries')
    parser.add_argument('indexLocation',type=str)
    parser.add_argument("vector",type=str)
    parser.add_argument('-v',action='store_true')
    
    
    args = parser.parse_args()

    if args.v:
        logging.getLogger().setLevel(logging.INFO)

    main(args.indexLocation,args.vector)