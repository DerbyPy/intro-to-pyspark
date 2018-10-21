from operator import add
from pyspark import SparkContext

if __name__ == "__main__":
    sc = SparkContext(appName="word_count")

    lines = sc.textFile("./data/so-developer-survey-2017/README_2017.txt")

    counts = lines.flatMap(lambda r: [(w.lower(), 1) for w in r.split()]) \
                  .reduceByKey(add)

    output = counts.collect()

    for (word, count) in output:
        print("{0:s}: {1:d}".format(word, count))

    sc.stop()
