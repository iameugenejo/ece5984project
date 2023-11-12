from datetime import datetime
from airflow.decorators import dag, task
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
    # extract
    temp_file = extract.download_unprocessed()
    # upload extracted to s3
    s3_file_path_unprocessed = extract.upload_unprocessed(temp_file)

    # transform
    df = transform.transform(s3_file_path_unprocessed)
    # upload transformed to s3
    s3_file_path_transformed = transform.upload_transformed(df)

    # construct train tasks
    train_tasks = []
    for target_cls in TRAIN_TARGETS:
        @task(task_id=target_cls.__name__)
        def train_task(s3file_path: str):
            load.train(s3file_path, target_cls)

        train_tasks.append(train_task(s3_file_path_transformed))

    # load
    load.save_result_to_db(*train_tasks)


final()
