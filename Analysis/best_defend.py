from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
from csv import reader

if __name__ == "__main__":
	sc = SparkContext()
	
	def toCSVLine(data):
		return ','.join(str(d) for d in data)

	def charconverter(i):
		return i.encode('utf-8')

	def score(items):
		count = 0
		items = map(int, items)
		for i,j in zip(items, items[1:]):
			if (j-i) ==1:
				count += 1
		return count
    
	lines = sc.textFile(sys.argv[1], 1)

	lines = lines.map(lambda x: (charconverter(x)))

	lines = lines.mapPartitions(lambda x: reader(x))
	
	lines = lines.map(lambda x: (x[0][:4], x[1], x[10], x[29]))

	winners = lines.filter(lambda x: x[3] == 'F')

	total_titles = winners.map(lambda x : (x[2], 1)).reduceByKey(add)

	total_titles = total_titles.map(toCSVLine)
	
	total_titles.saveAsTextFile('total_titles_wta.csv')

	defend_points = winners.map(lambda x: ((x[2], x[1]), x[0])).groupByKey().map(lambda x: (x[0], list(x[1]))).map(lambda x : (x[0][0], score(x[1]))).reduceByKey(add).takeOrdered(10, key  = lambda x: -x[1])


	defend_points = sc.parallelize(defend_points)		

	defend_points = defend_points.map(toCSVLine)	

	defend_points.saveAsTextFile('defend_points_wta.csv')