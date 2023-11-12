#!/usr/bin/env python3

import os
import pandas as pd
import numpy as np
from domain import Field, File, RemoteFile
from sklearn import feature_extraction
from sklearn import model_selection
from sklearn import metrics
from airflow.decorators import task
from airflow.models import Variable
from s3fs.core import S3FileSystem
import pickle
from sqlalchemy import create_engine


def train(s3_file_path_transformed: str, cls, ds=None, **kwargs) -> pd.DataFrame:
    print('training {}'.format(cls.__name__))

    incremental = False
    s3_file_path_trained = '{}/{}'.format(RemoteFile.DataWarehousePath, File.TrainedFileNameFormat.format(cls.__name__))

    s3 = S3FileSystem()

    # new data
    df = pd.read_pickle(s3.open(s3_file_path_transformed))  # type: pd.DataFrame

    clf = cls()
    if hasattr(clf, "partial_fit"):
        # load previously trained file from s3 if exists
        if s3.exists(s3_file_path_trained):
            clf = pickle.load(s3.open(s3_file_path_trained))
            incremental = True

    tfidf = feature_extraction.text.TfidfVectorizer()
    tv = tfidf.fit_transform(df[Field.JobDescriptionTokens])

    if incremental:
        scorer = metrics.check_scoring(clf)
        scores = scorer(clf, tv.toarray(), df[Field.JobTitleNormalized])
    else:
        scores = model_selection.cross_val_score(clf, tv.toarray(), df[Field.JobTitleNormalized], error_score='raise', scoring='accuracy', n_jobs=-1)

    mean_score, std_score = np.mean(scores), np.std(scores)

    result = pd.DataFrame([ds, cls.__name__, mean_score, std_score], index=['date', 'class', 'mean accuracy', 'std accuracy'])
    print(result)

    # fit model
    clf.fit(tv.toarray(), df[Field.JobTitleNormalized])

    # save model to s3
    with s3.open(s3_file_path_trained, 'wb') as f:
        f.write(pickle.dumps(clf))

    return result


@task()
def save_result_to_db(*dfs):
    db_user = Variable.get("DB_USER", default_var=None)
    db_pass = Variable.get("DB_PASSWORD", default_var=None)
    db_url = Variable.get("DB_URL", default_var="database-dataeng.cwgvgleixj0c.us-east-1.rds.amazonaws.com")
    db_db = Variable.get("DB_DB", default_var="eugenejj")

    assert not not db_user, "DB_USER variable must be set"
    assert not not db_pass, "DB_PASSWORD variable must be set"

    # create sqlalchemy engine
    engine = create_engine("mysql+pymysql://{user}:{pw}@{endpnt}"
                           .format(user=db_user,
                                   pw=db_pass,
                                   endpnt=db_url))

    engine.execute("CREATE DATABASE {db}"
                   .format(db=db_db))  # Insert pid here

    engine = create_engine("mysql+pymysql://{user}:{pw}@{endpnt}/{db}"
                           .format(user=db_user,
                                   pw=db_pass,
                                   endpnt=db_url,
                                   db=db_db))

    for df in dfs:  # type: pd.DataFrame
        df.to_sql('class_scores', con=engine, if_exists='append', chunksize=1000)
