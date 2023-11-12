from datetime import datetime
from airflow.decorators import dag, task
import extract
import transform
import load
from sklearn import naive_bayes, neighbors, tree, neural_network, linear_model, svm


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

    # load
    load.save_result_to_db(
        task(task_id="train_svc")(load.train)(s3_file_path_transformed=s3_file_path_transformed, cls=svm.SVC),
        task(task_id="train_ridge")(load.train)(s3_file_path_transformed=s3_file_path_transformed, cls=linear_model.RidgeClassifier),
        task(task_id="train_sgd")(load.train)(s3_file_path_transformed=s3_file_path_transformed, cls=linear_model.SGDClassifier),
        task(task_id="train_mlp")(load.train)(s3_file_path_transformed=s3_file_path_transformed, cls=neural_network.MLPClassifier),
        task(task_id="train_decision_tree")(load.train)(s3_file_path_transformed=s3_file_path_transformed, cls=tree.DecisionTreeClassifier),
        task(task_id="train_kn")(load.train)(s3_file_path_transformed=s3_file_path_transformed, cls=neighbors.KNeighborsClassifier),
        task(task_id="train_bernoulli")(load.train)(s3_file_path_transformed=s3_file_path_transformed, cls=naive_bayes.BernoulliNB),
        task(task_id="train_complement")(load.train)(s3_file_path_transformed=s3_file_path_transformed, cls=naive_bayes.ComplementNB),
        task(task_id="train_gaussian")(load.train)(s3_file_path_transformed=s3_file_path_transformed, cls=naive_bayes.GaussianNB),
        task(task_id="train_categorical")(load.train)(s3_file_path_transformed=s3_file_path_transformed, cls=naive_bayes.CategoricalNB),
        task(task_id="train_multinomial")(load.train)(s3_file_path_transformed=s3_file_path_transformed, cls=naive_bayes.MultinomialNB),
    )


final()
