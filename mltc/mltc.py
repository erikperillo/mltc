#!/usr/bin/env python3

import oarg
import pickle
import csv
import shutil
import msgtocsv
from nltk import PorterStemmer
from pyspark import SparkContext
from pyspark.mllib.feature import HashingTF, IDF
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import NaiveBayes, NaiveBayesModel
from pyspark.mllib.util import MLUtils

TRAIN_DATA_FILEPATH = "./data.csv"
MODEL_DIR = "./model"
CORPUS_FILE = "tfidf.pkl"

TRAIN_SPLIT = 0.7
TEST_SPLIT = 1.0 - TRAIN_SPLIT

DEBUG = False

def predict(model, data):
    pred = data.map(lambda p: (model.predict(p.features), p.label))
    return pred

def accuracy(pred, test):
    acc = 1.0*pred.filter(lambda x: x[0] == x[1]).count()/test.count()
    return acc 

def train(sc, train, model_dir=MODEL_DIR):
    #spliting data into training and test 
    #train, test = data.randomSplit([TRAIN_SPLIT, TEST_SPLIT])

    #training a naive bayes model
    model = NaiveBayes.train(train, 1.0)

    #saving model
    shutil.rmtree(model_dir, ignore_errors=True)
    model.save(sc, model_dir)

    return model

def prdd(rdd, label="rdd", debug=DEBUG):
    if debug:
        print(label, ":")
        rdd.foreach(print)
        print()

def csv_to_rdd(sc, filepath):
    data = sc.textFile(filepath)
    data = data.mapPartitions(lambda x: csv.reader(x))
    prdd(data, "csv_to_rdd")

    return data

def pre_proc(words_rdd):
    stemmer = PorterStemmer()
    return words_rdd.map(lambda x: list(map(stemmer.stem_word, x)))

def rdd_to_train(sc, rdd, num_feats=100, idf_file_load=False):
    #getting labels
    labels = rdd.map(lambda l: l[0])
    prdd(labels, "labels")

    #getting words and pre-processing them
    words = rdd.map(lambda doc: doc[1].split())
    prdd(words, "words")
    words = pre_proc(words)
    prdd(words, "stemmed words")

    #text frequency
    tf = HashingTF(numFeatures=num_feats).transform(words)
    prdd(tf, "tf")

    #getting idf
    if not idf_file_load:
        idf = IDF().fit(tf)
    else:
        idf = IDF().fit(tf)

    tfidf = idf.transform(tf)
    prdd(tfidf, "tfidf")

    #joining labels and words
    training = labels.zip(tfidf).map(lambda x: LabeledPoint(x[0], x[1]))
    prdd(training, "training")

    return training

#expects csv in format class,text
def csv_to_data(sc, filepath, num_feats=100, idf_file_load=False):
    data = csv_to_rdd(sc, filepath)
    data = rdd_to_train(sc, data, num_feats=num_feats, 
        idf_file_load=idf_file_load)

    return data

def main():
    sc = SparkContext(appName="PysparkNaiveBayes")
    sc.setLogLevel("ERROR")

    do_train = oarg.Oarg("-t --train", True, "perform training")
    train_file = oarg.Oarg("-T --train-file", TRAIN_DATA_FILEPATH, 
        "train csv file in format class,text")
    do_predict = oarg.Oarg("-p --predict", False, "perform predict")
    predict_file = oarg.Oarg("-P --predict-file", "", "predict file")
    num_feats = oarg.Oarg("-n --num-feats", 200, "number of features")
    hlp = oarg.Oarg("-h --help", False, "this help message")

    oarg.parse()

    if hlp.val:
        oarg.describe_args("options:")
        exit()

    if not do_predict.val and do_train.val:
        print("getting train rdd from csv '%s'..." % TRAIN_DATA_FILEPATH, 
        end="", flush=True) 
        data_rdd = csv_to_data(sc, TRAIN_DATA_FILEPATH, num_feats.val)
        print(" done.")

        #splitting between train and test
        train_rdd, test_rdd = data_rdd.randomSplit([TRAIN_SPLIT, TEST_SPLIT])

        print("training model and saving to '%s'..." % MODEL_DIR, end="",
            flush=True) 
        model = train(sc, train_rdd, MODEL_DIR)
        print(" done.")

        print("getting predicion...", end="", flush=True)
        pred = predict(model, test_rdd)
        print(" done.")

        print("getting accuracy...")
        acc = accuracy(pred, test_rdd)
        print("model accuracy = %.6f" % acc)

    elif do_predict.val:
        if not predict_file.val:
            print("must pass a file to predict label")
            exit()

        #translating text fo appropriate format
        line = msgtocsv.file_to_line(predict_file.val)
        with open("temp.csv", "w") as f:
            f.write(line)
        data_rdd = csv_to_data(sc, "temp.csv", num_feats.val, 
            idf_file_load=True)
        data_rdd.foreach(print)

        #loading prediction model
        model = NaiveBayesModel.load(sc, MODEL_DIR)
        pred = predict(model, data_rdd)
        pred.foreach(print)
        #acc = 1.0 * predictionAndLabel.filter(lambda x: x[0] == x[1]).count() / test_rdd.count()
        #print('sameModel accuracy {}'.format(acc))

if __name__ == "__main__":
    main()
