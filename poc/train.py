#!/usr/bin/env python3

import sys
from os import path
import pandas as pd
import numpy as np
from domain import Field, File
from sklearn import naive_bayes, feature_extraction, neighbors, tree, neural_network, linear_model, svm
from sklearn import model_selection
DIR = sys.path[0]


def train(cls):
    print('training {}'.format(cls.__name__))
    df = pd.read_pickle(path.join(DIR, File.CleanedFileName))  # type: pd.DataFrame

    clf = cls()

    tfidf = feature_extraction.text.TfidfVectorizer()
    tv = tfidf.fit_transform(df[Field.JobDescriptionTokens])

    scores = model_selection.cross_val_score(clf, tv.toarray(), df[Field.JobTitleNormalized], error_score='raise', scoring='accuracy', n_jobs=-1)

    mean_score, std_score = np.mean(scores), np.std(scores)
    print(
        pd.DataFrame([mean_score, std_score], index=['mean accuracy', 'std accuracy'])
    )


train(svm.SVC)
train(linear_model.RidgeClassifier)
train(linear_model.SGDClassifier)
train(neural_network.MLPClassifier)
train(tree.DecisionTreeClassifier)
train(neighbors.KNeighborsClassifier)
train(naive_bayes.BernoulliNB)
train(naive_bayes.ComplementNB)
train(naive_bayes.GaussianNB)
train(naive_bayes.CategoricalNB)
train(naive_bayes.MultinomialNB)
