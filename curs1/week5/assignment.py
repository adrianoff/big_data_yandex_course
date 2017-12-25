#!/usr/bin/env python
from pyspark import SparkConf, SparkContext
sc = SparkContext(conf=SparkConf().setAppName("MyApp").setMaster("local[2]"))

import re


def parse_article(line):
    try:
        article_id, text = unicode(line.rstrip()).split('\t', 1)
        text = re.sub("^\W+|\W+$", "", text, flags=re.UNICODE)
        words = re.split("\W*\s+\W*", text, flags=re.UNICODE)
        return words
    except ValueError as e:
        return []

wiki = sc.textFile("hdfs:///data/wiki/en_articles_part/articles-part", 16).map(parse_article)
print wiki.take(1)