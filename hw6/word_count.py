import re
import sys
from pyspark import SparkConf, SparkContext
import time


if __name__ == '__main__':
    conf = SparkConf()
    sc = SparkContext(conf=conf)
    lines = sc.textFile(sys.argv[1])

    first = time.time()

    # Students: Implement Word Count!
    words = lines.flatMap(lambda l: re.split(r'[^\w]+', l))
    pairs = words.map(lambda w: (w, 1))
    counts = pairs.reduceByKey(lambda n1, n2: n1 + n2)
    counts.saveAsTextFile('Word_count_ans.txt')

    last = time.time()

    print("Total program time: %.2f seconds" % (last - first))
    sc.stop()
