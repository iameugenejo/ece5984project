#!/usr/bin/env python3

import os
import tempfile
from domain import File, RemoteFile
from airflow.decorators import task
from s3fs.core import S3FileSystem
import requests
import pandas as pd
from domain import Field, RemoteFile
import pickle


@task()
def extract_and_upload():
    t = tempfile.mkdtemp(prefix="ece5984_")

    print('downloading the file to {}'.format(t))
    res = requests.get(RemoteFile.SourcePath)
    res.raise_for_status()

    filepath = os.path.join(t, File.UncleanedFileNameZip)

    with open(filepath, 'wb') as f:
        f.write(res.content)

    df = pd.read_csv(filepath, compression='zip')

    # pick only interested columns
    df = df[[Field.JobTitle, Field.JobDescription]]

    s3_file_path = '{}/{}'.format(RemoteFile.DataLakePath, File.UncleanedFileNameZip)
    print('uploading to {}'.format(s3_file_path))

    s3 = S3FileSystem()
    with s3.open(s3_file_path, 'wb') as f:
        f.write(pickle.dumps(df))

    return s3_file_path
