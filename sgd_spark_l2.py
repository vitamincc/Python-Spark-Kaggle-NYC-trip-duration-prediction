#!/usr/bin/env python
from math import sqrt
from numpy import array
from pyspark import SparkContext
from pyspark.mllib.regression import LabeledPoint, LinearRegressionWithSGD, LinearRegressionModel

# Load and parse the data
def parsePoint(line):
    values = [float(x) for x in line.replace(',', ' ').split(' ')]
    return LabeledPoint(values[-1], values[0:-1])

sc = SparkContext()
data = sc.textFile('train1_spark_nolabel.csv')

#split data to 80% training data, 20% test data
parsedData = data.map(parsePoint)
traindata, testdata = parsedData.randomSplit([0.8, 0.2])

#split traindata to 10 folds
train_size = traindata.count()
fold_size = train_size / 10

test1, rest1 = traindata.randomSplit([fold_size, (train_size-fold_size)])
train1 = traindata.subtract(test1)
train_size = rest1.count()

test2, rest2 = rest1.randomSplit([fold_size, (train_size-fold_size)])
train2 = traindata.subtract(test2)
train_size = rest2.count()

test3, rest3 = rest2.randomSplit([fold_size, (train_size-fold_size)])
train3 = traindata.subtract(test3)
train_size = rest3.count()

test4, rest4 = rest3.randomSplit([fold_size, (train_size-fold_size)])
train4 = traindata.subtract(test4)
train_size = rest4.count()

test5, rest5 = rest4.randomSplit([fold_size, (train_size-fold_size)])
train5 = traindata.subtract(test5)
train_size = rest5.count()

test6, rest6 = rest5.randomSplit([fold_size, (train_size-fold_size)])
train6 = traindata.subtract(test6)
train_size = rest6.count()
	
test7, rest7 = rest6.randomSplit([fold_size, (train_size-fold_size)])
train7 = traindata.subtract(test7)
train_size = rest7.count()

test8, rest8 = rest7.randomSplit([fold_size, (train_size-fold_size)])
train8 = traindata.subtract(test8)
train_size = rest8.count()

test9, rest9 = rest8.randomSplit([fold_size, (train_size-fold_size)])
train9 = traindata.subtract(test9)
train_size = rest9.count()

test10 = rest9
train10 = traindata.subtract(test10)

mean_RMSE = list()
def rmse_mae_gd(trainset, testset):
	#Stochastic gradient descent with l2
	model_sgd_l2 = LinearRegressionWithSGD.train(trainset, miniBatchFraction =0.00001, regParam=0.1, regType= 'l2', iterations=50, step=0.00000001)
	predicted = testset.map(lambda p: (p.label, model_sgd_l2.predict(p.features)))
	RMSE_l2 = sqrt(predicted.map(lambda vp: (vp[0] - vp[1])**2).reduce(lambda x, y: x + y) / predicted.count())
	MAE_l2 = predicted.map(lambda vp: abs(vp[0] - vp[1])).reduce(lambda x, y: x + y) / predicted.count()
	mean_RMSE.append(RMSE_l2)
	print ("Root Mean Squared Error for Stochastic Gradient Descent with l2: " + str(RMSE_l2))
	print ("Mean Absolute Error for Stochastic Gradient Descent with l2: " + str(MAE_l2))


			
rmse_mae_gd(train1, test1)
rmse_mae_gd(train2, test2)
rmse_mae_gd(train3, test3)
rmse_mae_gd(train4, test4)
rmse_mae_gd(train5, test5)
rmse_mae_gd(train6, test6)
rmse_mae_gd(train7, test7)
rmse_mae_gd(train8, test8)
rmse_mae_gd(train9, test9)
rmse_mae_gd(train10, test10)

print ("Mean Root Mean Squared Error for Stochastic Gradient Descent in 10 Fold CV:" + str(sum(mean_RMSE)/10))
x = rmse_mae_gd(traindata, testdata)













