#!/usr/bin/env python3
from kaggle.api.kaggle_api_extended import KaggleApi

FILENAME = 'Uncleaned_DS_jobs.csv'


def download():
    api = KaggleApi()
    api.authenticate()

    api.dataset_download_file("rashikrahmanpritom/data-science-job-posting-on-glassdoor", FILENAME, "./", quiet=False)


download()

