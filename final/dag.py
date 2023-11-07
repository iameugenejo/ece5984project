from datetime import datetime
from airflow.decorators import dag
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
        load.train(s3_file_path_transformed, svm.SVC),
        load.train(s3_file_path_transformed, linear_model.RidgeClassifier),
        load.train(s3_file_path_transformed, linear_model.SGDClassifier),
        load.train(s3_file_path_transformed, neural_network.MLPClassifier),
        load.train(s3_file_path_transformed, tree.DecisionTreeClassifier),
        load.train(s3_file_path_transformed, neighbors.KNeighborsClassifier),
        load.train(s3_file_path_transformed, naive_bayes.BernoulliNB),
        load.train(s3_file_path_transformed, naive_bayes.ComplementNB),
        load.train(s3_file_path_transformed, naive_bayes.GaussianNB),
        load.train(s3_file_path_transformed, naive_bayes.CategoricalNB),
        load.train(s3_file_path_transformed, naive_bayes.MultinomialNB),
    )

