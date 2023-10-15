#!/usr/bin/env python3
import pandas as pd
from tabulate import tabulate

FILENAME = 'Uncleaned_DS_jobs.csv.zip'


def eda():
    df = pd.read_csv("/tmp/{}".format(FILENAME))
    desc_less_df = df.drop(columns=['Job Description'])
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


eda()
