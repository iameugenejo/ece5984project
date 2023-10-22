#!/usr/bin/env python3

import sys
from os import path
import pandas as pd
from tabulate import tabulate
from domain import Field, File

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


eda()
