#!/usr/bin/env python3

import sys
sys.path.append(sys.path[0])

from os import path

import pandas
import pandas as pd
from fields import Field
from aggregation import *


DIR = sys.path[0]
FILENAME = 'Uncleaned_DS_jobs.csv.zip'


def transform():
    df = pd.read_csv(path.join(DIR, FILENAME))

    # normalize job titles
    normalize_job_titles(df, log=True)


def normalize_job_titles(df: pandas.DataFrame, log: bool = False):
    if log:
        print('Count by {}'.format(Field.JobTitle))
        print(get_field_value_count(df, Field.JobTitle))
        print('Unique count of {}'.format(Field.JobTitle))
        print(get_field_unique_count(df, Field.JobTitle))

    df[Field.JobTitleNormalized] = df[Field.JobTitle].replace(regex=r' [-\(].*$', value='')

    if log:
        print('Count by {}'.format(Field.JobTitleNormalized))
        print(get_field_value_count(df, Field.JobTitleNormalized))
        print('Unique count of {}'.format(Field.JobTitleNormalized))
        print(get_field_unique_count(df, Field.JobTitleNormalized))


transform()
