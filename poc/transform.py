#!/usr/bin/env python3
import os
import re
import sys
import typing
from os import path
import pandas as pd
from domain import Field, File
from aggregation import *
import nltk
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from nltk.tag import pos_tag
import string
import pickle

sys.path.append(sys.path[0])

DIR = sys.path[0]
DIR_wh = 's3://ece5984-bucket-eugenejj/Project'  # TODO confirm the final destination
DESTINATION = 'local'

nltk.download('wordnet')
lemmatizer = WordNetLemmatizer()

target_job_titles = [
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

manager_pattern = re.compile(r'(manager|management|director|president|vp)')
senior_pattern = re.compile(r'(senior|sr|experienced|staff|lead|principal)')

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

    def match_title(title: str) -> str:
        if not any(t in title for t in target_job_titles):
            return title

        if manager_pattern.match(title):
            return 'data science manager'
        elif 'machine learning' in title:
            return 'machine learning engineer'
        elif senior_pattern.match(title):
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

    df[Field.JobTitleNormalized] = df[Field.JobTitle].str.lower()
    df[Field.JobTitleNormalized].replace(regex=r' [-\(].*$', value='', inplace=True)
    df[Field.JobTitleNormalized] = df[Field.JobTitleNormalized].apply(match_title)

    print('Count by {} (10)'.format(Field.JobTitleNormalized))
    print(get_field_value_count(df, Field.JobTitleNormalized)[:10])
    print('Statistics of {}'.format(Field.JobTitleNormalized))
    print(df[Field.JobTitleNormalized].describe())


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

    token_counts = []

    def tokenize(text) -> str:
        if text is None:
            return ''

        if not isinstance(text, str):
            print('invalid type {}'.format(type(text)))
            return ''

        tokens = [word.lower() for word in nltk.word_tokenize(text) if len(word) > 1]
        nonstop = [word for word in tokens if word not in sws]
        lemmatized = lemmatize_all(nonstop)
        token_counts.append(len(lemmatized))
        return ' '.join(lemmatized)

    df[Field.JobDescriptionTokens] = df[Field.JobDescription].transform(tokenize)

    print('Tokenized (10):')
    print(df[Field.JobDescriptionTokens][:10])

    print('Token counts of {}'.format(Field.JobDescriptionTokens))
    print(pd.DataFrame(token_counts).describe())


def lemmatize_all(words) -> typing.List[str]:
    lemmatized = []
    wnl = WordNetLemmatizer()
    for word, tag in pos_tag(words):
        if tag.startswith("NN"):
            lemmatized.append(wnl.lemmatize(word, pos='n'))
        elif tag.startswith('VB'):
            lemmatized.append(wnl.lemmatize(word, pos='v'))
        elif tag.startswith('JJ'):
            lemmatized.append(wnl.lemmatize(word, pos='a'))
        else:
            lemmatized.append(word)

    return lemmatized


transform()
