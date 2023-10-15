#!/usr/bin/env python3
import pandas as pd
import nltk
from nltk.corpus import stopwords
from nltk.stem.porter import PorterStemmer
import string

FILENAME = 'Uncleaned_DS_jobs.csv.zip'


stemmer = PorterStemmer()


def transform():
    df = pd.read_csv("/tmp/{}".format(FILENAME))

    sws = set(stopwords.words('english'))

    with open('stopwords.txt') as f:  # more english stop words
        sws |= set([line.strip() for line in f.readlines() if line.strip()])

    sws |= set(string.punctuation)  # remove punctuations
    sws |= set(string.digits)  # remove digits

    def tokenize(text):
        tokens = [word for word in nltk.word_tokenize(text) if len(word) > 1]
        nonstop = [word for word in tokens if word not in sws]
        stems = [stemmer.stem(item) for item in nonstop]
        return stems

    # TODO


transform()
