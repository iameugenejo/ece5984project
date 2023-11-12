#!/usr/bin/env python3

import os
import tempfile
from domain import File, RemoteFile
from airflow.decorators import task
from s3fs.core import S3FileSystem
import requests


@task()
def download_unprocessed():
    t = tempfile.mkdtemp(prefix="ece5984_")

    print('downloading the file to {}'.format(t))
    res = requests.get(RemoteFile.SourcePath)
    res.raise_for_status()

    filepath = os.path.join(t, File.UncleanedFileNameZip)

    with open(filepath, 'wb') as f:
        f.write(res.content)

    return filepath


@task()
def upload_unprocessed(file_path: str) -> str:
    s3_file_path = '{}/{}'.format(RemoteFile.DataLakePath, File.UncleanedFileNameZip)
    print('uploading {} to {}'.format(file_path, s3_file_path))

    s3 = S3FileSystem()
    with open(file_path, mode='rb') as f:
        with s3.open(s3_file_path, 'wb') as rf:
            rf.write(f.read())

    return s3_file_path
