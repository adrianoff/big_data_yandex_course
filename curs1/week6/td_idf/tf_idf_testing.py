import math

ibm = (2.0/6.0) * (1.0/math.log(2.0))
print ibm


from sklearn.feature_extraction.text import TfidfVectorizer
corpus = [
    "ibm vipusk first computer computer ibm",
    "computer system linux kernel kernel linux",
    "windows microsoft vipusk system okna",
    "windows mac grant klava linux dos"
]

vectorizer = TfidfVectorizer(min_df=1)
X = vectorizer.fit_transform(corpus)
idf = vectorizer.idf_
#print dict(zip(vectorizer.get_feature_names(), idf))

feature_names = vectorizer.get_feature_names()
doc = 1
feature_index = X[doc,:].nonzero()[1]
tfidf_scores = zip(feature_index, [X[doc, x] for x in feature_index])
for w, s in [(feature_names[i], s) for (i, s) in tfidf_scores]:
    print w, s


print "#########"


from textblob import TextBlob as tb


def tf(word, blob):
    return (blob.words.count(word)*1.0) / (len(blob.words)*1.0)


def n_containing(word, bloblist):
    return sum(1 for blob in bloblist if word in blob.words)


def tfidf(word, blob, bloblist):

    def idf(word, bloblist):
        return 1.0 / math.log(1.0 + n_containing(word, bloblist))

    return tf(word, blob) * idf(word, bloblist)


document1 = tb("ibm vipusk first computer computer ibm")
document2 = tb("computer system linux kernel kernel linux")
document3 = tb("windows microsoft vipusk system okna")
document4 = tb("windows mac grant klava linux dos")


bloblist = [document1, document2, document3, document4]
for i, blob in enumerate(bloblist):
    print("Top words in document {}".format(i + 1))
    scores = {word: tfidf(word, blob, bloblist) for word in blob.words}
    sorted_words = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    for word, score in sorted_words[:3]:
        print("\tWord: {}, TF-IDF: {}".format(word, score, 5))