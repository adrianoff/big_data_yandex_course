from pyspark import SparkConf, SparkContext
import time

t1 = time.time()

sc = SparkContext(conf=SparkConf().setAppName("MyApp").setMaster("local"))


def parse_edge(s):
    user, follower = s.split("\t")
    return (int(user), int(follower))


def step(item):
    prev_v, prev_d, next_v, path = item[0], item[1][0][0], item[1][1], item[1][0][1]
    new_path = []
    for p in path:
        new_path.append(p)
    new_path.append(prev_v)
    return (next_v, (prev_d + 1, tuple(new_path)))


def complete(item):
    v = item[0]
    old_d = item[1][0][0] if item[1][0] is not None else None
    new_d = None
    path = None
    if item[1][1] is not None:
        new_d = item[1][1][0]
        path = item[1][1][1]
    return (v, (old_d if old_d is not None else new_d, path if path is not None else ()))


# n = 5  # number of partitions
edges = sc.textFile("./twitter_sample   .txt").map(parse_edge).cache()
n = 400  # number of partitions
forward_edges = edges.map(lambda e: (e[1], e[0])).partitionBy(n).persist()

x = 12
target = 34
d = 0
path = ()
found_path = ()
distances = sc.parallelize([(x, (d, path))]).partitionBy(n)

while True:
    # import pdb; pdb.set_trace()
    candidates = distances.join(forward_edges, n).map(step)
    candidates = candidates.reduceByKey(lambda v1, v2: v1 if len(v1[1]) < len(v2[1]) else v2)
    # check if target is in cansidates; if yes break
    found = candidates.filter(lambda c: c[0] == target)
    # import pdb; pdb.set_trace()
    if found.count() > 0:
        found_path = found.take(1)[0][1][1]
        break

    # if not keep going
    new_distances = distances.fullOuterJoin(candidates, n).map(complete, True).persist()
    # import pdb; pdb.set_trace()
    count = new_distances.filter(lambda i: i[1][0] == d + 1).count()
    if count > 0:
        d += 1
        distances = new_distances
        # print("d = ", d, "count = ", count)
    else:
        break

if len(found_path) == 0:
    print "path not found"
else:
    print (','.join(map(str, found_path)) + ',' + str(target))
t2 = time.time()
print (t2 - t1)
