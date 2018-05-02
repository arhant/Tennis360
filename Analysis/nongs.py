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

	def winpoints(i):
		premier = ['P','PM','W']
		if i in premier:
			return 2
		else:
			return 1

	def earningpoints(i):
		if i >= 20000000:
			return 20
		elif i >= 15000000 and i < 20000000:
			return 15
		elif i >= 10000000 and i < 15000000:
			return 10
		elif i >= 8000000 and i < 10000000:
			return 8
		elif i >= 6000000 and i < 8000000:
			return 6
		elif i >= 2000000 and i < 6000000:
			return 2
		else:
			return 1
			
	def rankpoints(i):
		if i == 1:
			return 100
		elif i >= 2 and i <= 5:
			return 75
		elif i > 5 and i <= 10:
			return 50
		elif i > 10 and i <= 20:
			return 40
		elif i > 20 and i <= 30:
			return 20
		elif i > 30 and i <= 50:
			return 10
		else:
			return 2

    
	lines = sc.textFile(sys.argv[1], 1)

	rows = sc.textFile(sys.argv[2], 1)
	
	inputs = sc.textFile(sys.argv[3], 1)
	
	earnings = sc.textFile(sys.argv[4], 1)

	lines = lines.map(lambda x: (charconverter(x)))

	lines = lines.mapPartitions(lambda x: reader(x))

	rows = rows.map(lambda x: (charconverter(x)))

	rows = rows.mapPartitions(lambda x: reader(x))
	
	lines = lines.map(lambda x: (x[2], int(x[1])))

	rows = rows.map(lambda x: (x[0], x[1], x[2]))

	ranks = lines.groupByKey().map(lambda x: (x[0], min(list(x[1]))))
	
	players = rows.map(lambda x: (x[0], str(x[1] + " " + x[2])))

	best_ranks = players.join(ranks)

	best_ranks = best_ranks.map(lambda x: (x[1])).map(lambda x: (x[0], rankpoints(x[1])))

	inputs = inputs.map(lambda x: (charconverter(x)))

	inputs = inputs.mapPartitions(lambda x: reader(x))
	
	inputs = inputs.map(lambda x: (x[4], x[10], x[29]))

	winners = inputs.filter(lambda x: x[2] == 'F').map(lambda x: (x[1], x[0]))

	gs_winners = winners.filter(lambda x: x[1] == 'G')

	oth_winners = winners.filter(lambda x: x[1] != 'G').filter(lambda x: x[1] != 'D').filter(lambda x: x[1] != 'O')

	both=oth_winners.join(gs_winners)
	
	result=oth_winners.subtractByKey(both)
	
	winning_sum = result.map(lambda x: (x[0], int(winpoints(x[1])))).reduceByKey(add)

	both2 = best_ranks.join(winning_sum)

	temp = best_ranks.subtractByKey(both2)

	best_ranked = best_ranks.subtractByKey(temp)
	
	earnings = earnings.map(lambda x: (charconverter(x)))

	earnings = earnings.mapPartitions(lambda x: reader(x))
	
	earnings = earnings.map(lambda x: (x[1], int(x[3]))).map(lambda x: (x[0], earningpoints(x[1])))

	both3 = earnings.join(winning_sum)

	temp2 = earnings.subtractByKey(both3)

	earning_points = earnings.subtractByKey(temp2)

	points = best_ranked.join(winning_sum)

	points = points.map(lambda x: (x[0], sum(x[1])))

	points = points.join(earning_points)
	
	points = points.map(lambda x: (x[0], sum(x[1])))
	
	best_ranks = best_ranks.map(toCSVLine)
		
	best_ranks.saveAsTextFile('best_ranks_wta.csv')

	points = points.map(toCSVLine)

	points.saveAsTextFile('nongs_points.csv')

	                     