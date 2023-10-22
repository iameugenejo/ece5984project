#!/usr/bin/env python3

import sys
from kaggle.api.kaggle_api_extended import KaggleApi
from domain import File

DIR = sys.path[0]


def download():
    api = KaggleApi()
    api.authenticate()

    api.dataset_download_file("rashikrahmanpritom/data-science-job-posting-on-glassdoor", File.UncleanedFileName, DIR, quiet=False)


download()

