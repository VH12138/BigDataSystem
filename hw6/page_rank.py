import sys
from pyspark import SparkConf, SparkContext
import time
import re
import numpy as np


# function that maps an iterator of nodes to a numpy array
def nodesToVec(nodes, deg, n):
    vec = np.zeros((1, n))
    for node in nodes:
        if (node not in deg): continue
        vec[0][node - 1] = 1. / deg[node]
    return vec


if __name__ == '__main__':

    # Create Spark context.
    conf = SparkConf()
    sc = SparkContext(conf=conf)
    lines = sc.textFile(sys.argv[1])

    first = time.time()

    # Students: Implement PageRank!
    # Setup program constants
    num_steps = 100
    beta = 0.8

    # Create matrix M - broken into rows
    # Convert lines to pairs
    pairs = lines.map(lambda l: tuple([int(num) for num in re.split('[ |\t]', l)])).distinct()
    reverse_pairs = pairs.map(lambda p: tuple(reversed(p)))
    # Count the number of elements in the graph
    num_elem = pairs.flatMap(lambda p: p).distinct().count()
    # Find the number of outgoing edges from each node
    outgoing_count_dict = pairs.countByKey()
    # Create a row of M
    M = reverse_pairs.groupByKey().map(lambda (k, v): (k, nodesToVec(v, outgoing_count_dict, num_elem)))

    # Initialize pageRank vector r
    r = np.ones((num_elem, 1)) / num_elem
    r_prev = r.copy()

    # Calculate teleport probability
    tele_prob = (1. - beta) / num_elem

    # Iterate through the number of steps
    for _ in range(num_steps):
        rdd_r = M.map(lambda (k, v): (k, (v.dot(r_prev) * beta)[0][0]))
        r[:] = tele_prob

        for (node, val) in rdd_r.collect():
            r[node - 1][0] += val

	# Swap the assignments
	temp = r_prev
	r_prev = r
	r = temp

    #print("5 highest:", highest[:5])

    last = time.time()

    print("Total program time: %.2f seconds" % (last - first))
    sc.stop()
