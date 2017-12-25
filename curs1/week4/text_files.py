from collections import namedtuple
from pyspark import SparkContext

sc = SparkContext()
Record = namedtuple('Record', ['date', 'open', 'high', 'low', 'close', 'adj_close', 'volume'])


def parse_record(s):
    fields = s.split(',')
    return Record(fields[0], *map(float, fields[1:7]))

raw_data = sc.textFile('nasdaq.csv')
parsed_data = raw_data.map(parse_record).cache()
print parsed_data.take(1)

print parsed_data.map(lambda x: x.date).min()
print parsed_data.map(lambda x: x.date).max()
print parsed_data.map(lambda x: x.volume).sum()

with_month_data = parsed_data.map(lambda x: (x.date[:7], x))
print with_month_data.take(1)

by_month_data = with_month_data.mapValues(lambda x: x.volume)
print by_month_data.take(1)

by_month_data = by_month_data.reduceByKey(lambda x, y: x + y)
print by_month_data.take(1)

result_data = by_month_data.map(lambda t: ",".join(map(str, t)))
print result_data.take(1)

#result_data.saveAsTextFile("out")
result_data.repartition(1).saveAsTextFile("out")
