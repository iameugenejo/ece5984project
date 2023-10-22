#!/usr/bin/env python3
import os
import sys
from os import path
import pandas as pd
from domain import Field, File
from aggregation import *
import nltk
from nltk.corpus import stopwords
from nltk.stem.porter import PorterStemmer
import string
import pickle

sys.path.append(sys.path[0])

DIR = sys.path[0]
DIR_wh = 's3://ece5984-bucket-eugenejj/Project'  # TODO confirm the final destination
DESTINATION = 'local'


stemmer = PorterStemmer()


def transform():
    df = pd.read_csv(path.join(DIR, File.UncleanedFileName))

    # normalize job titles
    normalize_job_titles(df)

    # tokenize job descriptions
    tokenize_job_descriptions(df)

    if DESTINATION == 'local':
        save_to_local(df)
    else:
        save_to_s3(df)


def save_to_s3(df: pd.DataFrame):
    """
        push the cleaned data to S3 bucket warehouse

    :param df: cleaned dataframe
    """
    print('saving cleaned data to S3 bucket warehouse')

    from s3fs.core import S3FileSystem
    s3 = S3FileSystem()
    with s3.open('{}/{}'.format(DIR_wh, File.CleanedFileName), 'wb') as f:
        f.write(pickle.dumps(df))


def save_to_local(df: pd.DataFrame):
    """
        save the cleaned data to local file system

    :param df: cleaned dataframe
    """
    print('saving cleaned data to local file system')

    with open(path.join(DIR, File.CleanedFileName), 'wb') as f:
        f.write(pickle.dumps(df))


def normalize_job_titles(df: pd.DataFrame):
    """
        populates a new field with normalized job title

    :param df: dataframe
    """
    print('---------------')
    print('Normalizing {}'.format(Field.JobTitle))
    print('---------------')
    print('Count by {} (10)'.format(Field.JobTitle))
    print(get_field_value_count(df, Field.JobTitle)[:10])
    print('Unique count of {}'.format(Field.JobTitle))
    print(get_field_unique_count(df, Field.JobTitle))

    df[Field.JobTitleNormalized] = df[Field.JobTitle].replace(regex=r' [-\(].*$', value='')

    print('Count by {} (10)'.format(Field.JobTitleNormalized))
    print(get_field_value_count(df, Field.JobTitleNormalized)[:10])
    print('Unique count of {}'.format(Field.JobTitleNormalized))
    print(get_field_unique_count(df, Field.JobTitleNormalized))


def tokenize_job_descriptions(df: pd.DataFrame):
    """
        populates a new field with tokenized job description after stopwords are removed and remaining words are stemmed

    :param df: dataframe
    """

    print('---------------')
    print('Tokenizing {}'.format(Field.JobDescription))
    print('---------------')

    sws = set(stopwords.words('english'))
    # merge stopwords from https://www.ranks.nl/stopwords
    with open('stopwords-rank.nl.txt') as f:
        sws |= set([line.strip().lower() for line in f.readlines() if line.strip()])

    # merge stopwords from https://www.kaggle.com/datasets/rowhitswami/stopwords
    with open('stopwords-rowhitswami.txt') as f:
        sws |= set([line.strip().lower() for line in f.readlines() if line.strip()])

    sws |= set(string.punctuation)  # remove punctuations
    sws |= set(string.digits)  # remove digits

    def tokenize(text):
        tokens = [word.lower() for word in nltk.word_tokenize(text) if len(word) > 1]
        nonstop = [word for word in tokens if word not in sws]
        stems = [stemmer.stem(item) for item in nonstop]
        return ' '.join(stems)

    df[Field.JobDescriptionTokens] = df[Field.JobDescription].transform(tokenize)

    print('Tokenized (10):')
    print(df[Field.JobDescriptionTokens][:10])


transform()
