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

date_and_close_price = parsed_data.map(lambda r: (r.date, r.close))
print date_and_close_price.take(1)

from datetime import datetime, timedelta


def get_next_date(s):
    fmt = "%Y-%m-%d"
    return (datetime.strptime(s, fmt) + timedelta(days=1)).strftime(fmt)

print get_next_date("2017-01-03")


date_and_prev_close_price = parsed_data.map(lambda r: (get_next_date(r.date), r.close))
print date_and_prev_close_price.take(3)

joined = date_and_close_price.join(date_and_prev_close_price)
print joined.lookup("2017-01-04")




returns = joined.mapValues(lambda p: (p[0] / p[1] - 1.0) * 100.0)
print returns.lookup('2017-01-04')

print returns.sortByKey().take(3)