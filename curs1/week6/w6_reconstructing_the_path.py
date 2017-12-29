from pyspark import SparkConf, SparkContext

sc = SparkContext(conf=SparkConf().setAppName("MyApp").setMaster("local"))


def parse_edge(s):
    user, follower = s.split("\t")
    return (int(user), int(follower))


def step(item):
    prev_v, prev_d, next_v = item[0], item[1][0], item[1][1]
    return (next_v, prev_d + 1)


def complete(item):
    v, old_d, new_d = item[0], item[1][0], item[1][1]
    return (v, old_d if old_d is not None else new_d)


n = 5  # number of partitions
edges = sc.textFile("./twitter_sample_short.txt")
example = edges.take(1)
edges = edges.map(parse_edge).cache()
example = edges.take(1)

forward_edges = edges.map(lambda e: (e[1], e[0])).partitionBy(n).persist()
example = forward_edges.take(1)

x = 12
d = 0
distances = sc.parallelize([(x, d)]).partitionBy(n)
while True:
    candidates = distances.join(forward_edges, n)
    example = candidates.collect()
    candidates = candidates.map(step)
    example = candidates.collect()

    new_distances = distances.fullOuterJoin(candidates, n)
    example = new_distances.collect()
    new_distances = new_distances.map(complete, True).persist()
    example = new_distances.collect()

    count = new_distances.filter(lambda i: i[1] == d + 1).count()
    if count > 0:
        d += 1
        distances = new_distances
        example = distances.collect()
        print("d = ", d, "count = ", count)
    else:
        break
