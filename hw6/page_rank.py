import sys
import time
from typing import Tuple, List, Iterable
from pyspark import SparkConf, SparkContext


# CONSTANTS
BETA = 0.8
MAX_ITERATIONS = 100


def line_to_pair(line: str) -> Tuple[int, int]:
    tokens = line.split()
    return int(tokens[0]), int(tokens[1])


def to_degree(item: Tuple[int, Iterable[int]]) -> List[Tuple[int, Tuple[int, float]]]:
    source = item[0]
    destinations = list(item[1])

    out_degree = len(destinations)

    result = []
    for destination in destinations:
        result.append((destination, (source, 1 / out_degree)))

    return result


def multiplication_function(row: Tuple[int, List[Tuple[int, float]]]) -> Tuple[int, float]:
    destination = row[0]
    degrees = {key: value for key, value in row[1]}

    # compute dot product of "previous_row" x "this_row"
    dot_product = 0
    for j, value in enumerate(previous_r):
        if (j + 1) in degrees:
            dot_product += value * degrees[j + 1]

    return destination - 1, dot_product

if __name__ == '__main__':
    
    # Create Spark context.
    conf = SparkConf()
    sc = SparkContext(conf=conf)
    graph = sc.textFile(sys.argv[1])

    first = time.time()

    # Students: Implement PageRank!
    edges = graph.map(line_to_pair)
    matrix = edges.distinct().groupByKey().flatMap(to_degree).groupByKey()
    # matrix now in the form of (dest, [..., (source, degree), ...])

    n = edges.groupByKey().count()

    r = [1 / n for _ in range(0, n)]

    for i in range(0, MAX_ITERATIONS):
        previous_r = r.copy()
        r_temp = matrix.map(multiplication_function).collect()
        r_temp.sort(key=lambda x: x[0])
        r = [value * BETA + (1 - BETA) / n for _, value in r_temp]

    indices = [(i, r[i]) for i in range(0, n)]
    indices.sort(key=lambda x: x[1])
    sorted_indices = [x[0] + 1 for x in indices]

    # Top 5 nodes with highest page rank
    print('\n\nTOP 5 NODES (highest page rank)')
    print('===============================')
    print(list(reversed(sorted_indices[-5:])))

    # Top 5 nodes with lowest page rank
    print('\nLOWEST 5 NODES (lowest page rank)')
    print('=================================')
    print(sorted_indices[:5])
    print('\n\n')

    #print("5 highest:", highest[:5])

    last = time.time()

    print("Total program time: %.2f seconds" % (last - first))
    sc.stop()
