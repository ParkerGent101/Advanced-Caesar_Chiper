from pyspark import SparkContext
import numpy as np
import sys

beta = 0.8
MAX_ITER = 40

#Main pagerank algorthim
def page_rank(graph, MAX_ITER, beta):

    #Collects the graph data as an RDD set of edges
    edges = graph.map(lambda e: get_edge(e))


    #Collects the data such that each nodes outgoing edges
    #are grouped together
    degree = edges.groupByKey().mapValues(list)

    #Converts string values in degree to float and converts the 
    #iterable of strings to set of floats. Then sorts degree by key
    degree = degree.map(lambda w: (int(w[0]), set(float(i) for i in w[1]))).sortByKey()

    #Gets the number of nodes in the graph
    values = degree.count()

    #Constructs matrix based on outlinks of each node. If a 
    #node has an outlink to another the probability is assigned
    #1.0 / outdegree, otherwise it is assigned 0
    matrix = degree.map(lambda w: np.array([(1.0/len(w[1]) if (i+1 in w[1]) else 0) for i in range(values)]))

    #Initalize r as local data type
    r = np.zeros(values)
    r.fill(1.0/values)

    #Create vector of 1's and calculates the transport vector
    one = np.ones(values)
    transport = ((1.0 - beta) / values) * one

    #Pair each row of matrix with an index
    matrix.zipWithIndex()

    for i in range(MAX_ITER):
        #Pairs each row of matrix for this loop
        q = matrix.zipWithIndex()

        #Map each q value with r to incorporate current rank
        #to q
        q = q.map(lambda w: (w + (r[w[1]],)))

        #Calculate the weight of each node to other node rank
        w = q.map(lambda w: (1, w[0]*w[2]))

        #Sums the weight of each page by key
        e = w.reduceByKey(lambda n1, n2: n1 + n2)

        #Now the rank of the node can be updated using
        #the transport value 
        e = e.map(lambda w: transport + w[1]*beta)

        #Converts the RDD to an array
        r = np.array(e.collect()[0])

    return r

#Helper function to get edges when reading input
def get_edge(e):
    edge = e.split("\t")
    return (int(edge[0]), int(edge[1]))


#Main program
if __name__ == "__main__":

    #Create sparkContext
    sc = SparkContext("local[*]", "PageRank")

    #Graph data is collected
    graph = sc.textFile(sys.argv[1])

    #Run the page_rank algorithm that returns an array
    #r to output
    output = page_rank(graph, MAX_ITER, beta)

    #Sorts the array to quickly get bottom and top 5
    sorted_output = np.argsort(output)

    #Bottom and top 5 values
    bttm5 = list(sorted_output[:5])
    top5 = list(sorted_output[-5:])

    #Print output
    print("Top 5 values:")
    for i in range(5):
        g = top5.pop()
        print("{}:{}".format(g+1,output[g]))

    print("Bottom 5 Values:")
    for i in range(5):
        g = bttm5.pop()
        print("{}:{}".format(g+1,output[g]))

    sc.stop()

