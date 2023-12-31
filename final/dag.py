from datetime import datetime

import pandas as pd
from airflow.decorators import dag, task, task_group
import extract
import transform
import load
from sklearn import naive_bayes, neighbors, tree, neural_network, linear_model, svm

TRAIN_TARGETS = [
    svm.SVC,
    linear_model.RidgeClassifier,
    linear_model.SGDClassifier,
    neural_network.MLPClassifier,
    tree.DecisionTreeClassifier,
    neighbors.KNeighborsClassifier,
    naive_bayes.BernoulliNB,
    naive_bayes.ComplementNB,
    naive_bayes.GaussianNB,
    naive_bayes.CategoricalNB,
    naive_bayes.MultinomialNB,
]

@dag(
    schedule=None,
    start_date=datetime(2023, 11, 6),
    catchup=False,
)
def final():
    # extract and upload to s3
    s3_file_path_unprocessed = extract.extract_and_upload()

    # transform
    df = transform.transform(s3_file_path_unprocessed)
    # upload transformed to s3
    s3_file_path_transformed = transform.upload_transformed(df)

    @task_group(group_id='train')
    def train_group():
        train_results = []

        for target_cls in TRAIN_TARGETS:
            @task(task_id='train_{}'.format(target_cls.__name__))
            def train_task(s3file_path: str, cls) -> str:
                return load.train(s3file_path, cls).to_json()

            train_results.append(train_task(s3_file_path_transformed, target_cls))

        return train_results

    # load
    load.save_result_to_db(*train_group())


final()
