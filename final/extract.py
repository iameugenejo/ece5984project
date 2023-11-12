#!/usr/bin/env python3

import os
import tempfile
from kaggle.api.kaggle_api_extended import KaggleApi
from domain import File, RemoteFile
from airflow.decorators import task
from s3fs.core import S3FileSystem


@task()
def download_unprocessed():
    api = KaggleApi()
    api.authenticate()
    t = tempfile.mktemp(prefix="ece5984_")

    print('downloading the file to {}'.format(t))
    api.dataset_download_file(
        "rashikrahmanpritom/data-science-job-posting-on-glassdoor", File.UncleanedFileName, t, quiet=False)

    return os.path.join(t, File.UncleanedFileNameZip)


@task()
def upload_unprocessed(file_path: str) -> str:
    s3_file_path = '{}/{}'.format(RemoteFile.DataLakePath, File.UncleanedFileNameZip)
    print('uploading {} to {}'.format(file_path, s3_file_path))

    s3 = S3FileSystem()
    with open(file_path, mode='rb') as f:
        with s3.open(s3_file_path, 'wb') as rf:
            rf.write(f.read())

    return s3_file_path
