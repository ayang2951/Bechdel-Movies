import pyspark
from pyspark.sql import SparkSession

from pyspark.sql import Row
from pyspark.sql.types import *

from pyspark.mllib.classification import LogisticRegressionWithLBFGS, LogisticRegressionModel
from pyspark.mllib.regression import LabeledPoint

from pyspark.mllib.tree import RandomForest, RandomForestModel
from pyspark.mllib.util import MLUtils

from pyspark.mllib.stat import Statistics

from pyspark.sql.functions import exp, lit

import math

# all imports above

# create spark session
spark = SparkSession.builder.appName('Bechdel').getOrCreate()

# create the bechdel schema, read in the data
bechdel_schema = StructType([StructField("score", IntegerType(), False), StructField("pass", IntegerType(), False), StructField("id", StringType(), False), StructField("column1", StringType(), True)])
bechdel = spark.read.csv('BechdelProject/clean_bechdel/bechdel_clean.csv', sep=',', schema=bechdel_schema, header=False)

# create the imdb schema, read in the data
imdb_schema = StructType([StructField("year", IntegerType(), False), StructField("rating", FloatType(), False), StructField("votes", IntegerType(), False), StructField("id", StringType(), False), StructField("boxoffice", FloatType(), False), StructField("column1", StringType(), True)])
imdb = spark.read.csv('BechdelProject/imdb_ml/imdb_ml_data.csv', sep=',', schema=imdb_schema, header=False)

# define the logistic regression data and the random forest data
lr_data = imdb.join(bechdel, imdb.id == bechdel.id, 'inner').select(bechdel['pass'], imdb.year, imdb.rating, imdb.votes, imdb.boxoffice)
rf_data = imdb.join(bechdel, imdb.id == bechdel.id, 'inner').select(bechdel['score'], imdb.year, imdb.rating, imdb.votes, imdb.boxoffice)

# save this ML data into hdfs
lr_data.write.csv('BechdelProject/ml_data/lr_data.csv')
rf_data.write.csv('BechdelProject/ml_data/rf_data.csv')

# read in logistic regression data
lr_data = sc.textFile('BechdelProject/ml_data/lr_data.csv')

# process it using LabeledPoints
def process_row(line):
    values = [float(x) for x in line.split(',')]
    values[2] = values[2] - 1874
    return LabeledPoint(values[0], values[1:])

# process the data
labeled_data = lr_data.map(process_row)

# train (fit) the model
model = LogisticRegressionWithLBFGS.train(labeled_data)

# evaluate the model & print outputs
labelsAndPreds = labeled_data.map(lambda p: (p.label, model.predict(p.features)))
trainErr = labelsAndPreds.filter(lambda lp: lp[0] != lp[1]).count() / float(labeled_data.count())
print("Accuracy Rate = " + str(1 - trainErr))

print("Calculated Parameters: \n \t Year: " + str(model.weights[0]) + "\n \t IMDb Rating: " + str(model.weights[1]) + "\n \t IMDb Votes: " + str(model.weights[2]) + "\n \t Box Office: " + str(model.weights[3])  + "\n \t Intercept: " + str(model.intercept))

print("\n\n")

print("Odds Ratios: \n \t Year: " + str(math.exp(model.weights[0])) + "\n \t IMDb Rating: " + str(math.exp(model.weights[1])) + "\n \t IMDb Votes: " + str(math.exp(model.weights[2])) + "\n \t Box Office: " + str(math.exp(model.weights[3])))

# read in the random forest data
rf_data = sc.textFile('BechdelProject/ml_data/rf_data.csv')

# process
def process_row(line):
    values = [float(x) for x in line.split(',')]
    values[2] = values[2] - 1874
    return LabeledPoint(values[0], values[1:])

# processing
rf_labeled = rf_data.map(process_row)

# train (fit) the model
model = RandomForest.trainClassifier(rf_labeled, numClasses=4, categoricalFeaturesInfo={}, numTrees=3, featureSubsetStrategy="auto", impurity='gini', maxDepth=4, maxBins=32)

# print the outputs
predictions = model.predict(rf_labeled.map(lambda x: x.features))
labelsAndPredictions = rf_labeled.map(lambda lp: lp.label).zip(predictions)
testErr = labelsAndPredictions.filter(lambda lp: lp[0] != lp[1]).count() / float(rf_labeled.count())
print('Accuracy = ' + str(1 - testErr))
