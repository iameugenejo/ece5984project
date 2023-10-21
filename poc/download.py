#!/usr/bin/env python3

import sys
from kaggle.api.kaggle_api_extended import KaggleApi

DIR = sys.path[0]
FILENAME = 'Uncleaned_DS_jobs.csv'


def download():
    api = KaggleApi()
    api.authenticate()

    api.dataset_download_file("rashikrahmanpritom/data-science-job-posting-on-glassdoor", FILENAME, DIR, quiet=False)


download()

