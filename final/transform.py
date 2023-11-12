#!/usr/bin/env python3
import re
import sys
import typing
import pandas as pd
from domain import Field, File, RemoteFile
from aggregation import *
import nltk
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from nltk.tag import pos_tag
import string
import pickle
from airflow.decorators import task
from s3fs.core import S3FileSystem
import os

DIR = os.path.dirname(os.path.realpath(__file__))

sys.path.append(sys.path[0])

nltk.download('wordnet')
nltk.download('stopwords')
nltk.download('punkt')
nltk.download('averaged_perceptron_tagger')

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


@task()
def transform(s3_file_path_unprocessed: str) -> pd.DataFrame:
    s3 = S3FileSystem()
    df = pd.read_pickle(s3.open(s3_file_path_unprocessed, compression='zip'))

    # normalize job titles
    normalize_job_titles(df)

    # tokenize job descriptions
    tokenize_job_descriptions(df)

    return df


@task()
def upload_transformed(df: pd.DataFrame) -> str:
    """
        push the cleaned data to S3 bucket warehouse

        param df: cleaned dataframe
    """
    s3_file_path_transformed = '{}/{}'.format(RemoteFile.DataWarehousePath, File.CleanedFileName)
    print('saving cleaned data to S3 {}'.format(s3_file_path_transformed))

    s3 = S3FileSystem()
    with s3.open(s3_file_path_transformed, 'wb') as f:
        f.write(pickle.dumps(df))

    return s3_file_path_transformed


def normalize_job_titles(df: pd.DataFrame):

    df[Field.JobTitleNormalized] = df[Field.JobTitle].str.lower()
    df[Field.JobTitleNormalized].replace(regex=r' [-\(].*$', value='', inplace=True)

    print('Count by {} (10)'.format(Field.JobTitleNormalized))
    print(get_field_value_count(df, Field.JobTitleNormalized)[:10])
    print('Statistics of {}'.format(Field.JobTitleNormalized))
    print(df[Field.JobTitleNormalized].describe())


def normalize_job_titles_big_data(df: pd.DataFrame):
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
    with open(os.path.join(DIR, 'stopwords-rank.nl.txt')) as f:
        sws |= set([line.strip().lower() for line in f.readlines() if line.strip()])

    # merge stopwords from https://www.kaggle.com/datasets/rowhitswami/stopwords
    with open(os.path.join(DIR, 'stopwords-rowhitswami.txt')) as f:
        sws |= set([line.strip().lower() for line in f.readlines() if line.strip()])

    sws |= set(string.punctuation)  # remove punctuations
    sws |= set(string.digits)  # remove digits

    token_counts = []

    def tokenize(text):
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
