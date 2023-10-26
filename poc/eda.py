#!/usr/bin/env python3

import sys
from os import path
import pandas as pd
import numpy as np
from tabulate import tabulate
from domain import Field, File

import nltk
from nltk.corpus import stopwords
import string

import matplotlib
matplotlib.use('TkAgg')
import matplotlib.pyplot as plt

from collections import defaultdict, Counter

DIR = sys.path[0]


def eda():
    df = pd.read_csv(path.join(DIR, File.UncleanedFileName))
    desc_less_df = df.drop(columns=[Field.JobDescription])
    print(tabulate(desc_less_df.sample(10, random_state=5984), headers='keys', tablefmt='psql'))

    print("Basic Dataframe info")
    print(df.info())
    print("====================================")
    print("More detailed Dataframe info")
    print(df.describe().to_string())
    print("====================================")
    print("Number of Empty values in each column:")
    print(df.isnull().sum().sort_values(ascending=False))
    print("====================================")
    print("Number of Unique values in each column:")
    print(df.apply(pd.Series.nunique))
    print("====================================")
    print("Are there duplicate rows?")
    print(df.duplicated())
    print("====================================")

    print("Title Count")
    print(df.groupby(by=[Field.JobTitle])[Field.JobTitle].count().sort_values())
    print("====================================")


    print("Clean title")
    df[Field.JobTitleNormalized] = df[Field.JobTitle].str.lower()
    # create a predefined job title bucket
    job_titles = [
        'data scientist',
        'data engineer',
        'senior data scientist',
        'machine learning engineer',
        'machine learning scientist',
        'data analyst',
        'data science manager',
        'senior data analyst',
        'senior data engineer',
    ]
    senior = ['senior', 'sr', 'experienced', 'ii', 'iii', 'staff', 'lead', 'principal']
    manager = ['manager', 'management', 'director', 'president', 'vp']

    def match_title(title: str) -> str:
        if not any(t in title for t in job_titles):
            return title

        if any(key in title for key in manager):
            return 'data science manager'
        elif 'machine learning' in title:
            return 'machine learning engineer'
        elif any(key in title for key in senior):
            if 'engineer' in title:
                return 'senior data engineer'
            elif 'analyst' in title:
                return 'senior data analyst'
            elif 'scientist' in title:
                return 'senior data scientist'
        elif 'engineer' in title:
            return 'data engineer'
        elif 'analyst' in title:
            return 'data analyst'
        elif 'scientist' in title:
            return 'data scientist'
        else:
            return title

    df[Field.JobTitleNormalized] = df[Field.JobTitleNormalized].apply(match_title)
    print(df.groupby(by=[Field.JobTitleNormalized])[Field.JobTitleNormalized].count().sort_values())
    for k, v in df.groupby(by=[Field.JobTitleNormalized])[Field.JobTitleNormalized].count().sort_values().to_dict().items():
        print(f'{k}\t\t{v}')

    print("====================================")


    print("Job Description Histogram")
    df['Job Description'].str.len().hist()
    plt.savefig('../job_description_histogram.png')
    plt.clf()

    df['Job Description'].str.split().map(lambda x: len(x)).hist()
    plt.savefig('../job_description_word_histogram.png')
    plt.clf()

    df['Job Description'].str.split().apply(lambda x : [len(i) for i in x]).map(lambda x: np.mean(x)).hist()
    plt.savefig('../job_description_avg_word_len_histogram.png')
    plt.clf()

    # setup stop words
    sws = set(stopwords.words('english'))
    # merge stopwords from https://www.ranks.nl/stopwords
    with open('stopwords-rank.nl.txt') as f:
        sws |= set([line.strip().lower() for line in f.readlines() if line.strip()])

    # merge stopwords from https://www.kaggle.com/datasets/rowhitswami/stopwords
    with open('stopwords-rowhitswami.txt') as f:
        sws |= set([line.strip().lower() for line in f.readlines() if line.strip()])

    sws |= set(string.punctuation)  # remove punctuations
    sws |= set(string.digits)  # remove digits

    corpus = []
    jd = df['Job Description'].str.split()
    jd = jd.values.tolist()
    corpus = [word.lower() for i in jd for word in i]

    stop_words = defaultdict(int)
    for word in corpus:
        if word.lower() in sws:
            stop_words[word]+=1

    
    print("Stop words")
    for k, v in sorted(stop_words.items(), key=lambda kv: kv[1], reverse=True)[:10]:
        print(f'{k}\t\t{v}')
    print("====================================")
    sw = pd.DataFrame.from_dict(stop_words, orient='index')
    sw[0].nlargest(30).plot(kind='bar')
    plt.savefig('../job_description_top30_stopwords.png')
    plt.clf()


    counter=Counter(corpus)
    most=counter.most_common()

    keywords = {}
    for word,count in most:
        if (word.lower() not in sws):
            keywords[word] = count

    print("Keywords")
    for k, v in sorted(keywords.items(), key=lambda kv: kv[1], reverse=True)[:10]:
        print(f'{k}\t\t{v}')
    print("====================================")
    kw = pd.DataFrame.from_dict(keywords, orient='index')
    kw[0].nlargest(30).plot(kind='bar')
    plt.savefig('../job_description_top30_keywords.png')
    plt.clf()

eda()
