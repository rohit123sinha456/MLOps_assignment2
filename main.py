#collapse
import numpy as np
from sklearn.datasets import make_classification
import pyspark
from sklearn.metrics import accuracy_score

def distributed_svm_easy(data,w,n_iter):
    x = []
    y = []
    l_rate = 0.01
    for i in data:
        x.append(i[:-1])
        y.append(i[-1])

    for i, val in enumerate(x):
        val1 = np.dot(x[i],w)
        if(y[i]*val1 < 1):
            w = w + l_rate * ((y[i]*np.array(x[i])).tolist() - (2*(1/n_iter)*w))
        else:
            w = w + l_rate * (-2*(1/n_iter)*w)

    return [w]

def train(sc,data):
    rdd = sc.parallelize(data).cache()
    print(f"Our RDD has {rdd.getNumPartitions()} partitions.")
    w = np.zeros(len(X[0]))
    n_iter = 300
    for iter in range(n_iter):
        rdd_gradient = rdd.mapPartitions(lambda data : distributed_svm_easy(data,w,n_iter))
        lst = rdd_gradient.collect()
        sumws = 0
        for ws in lst:
            sumws += ws
        w = sumws/len(lst)
    return w

def test(x_test,weights):
    y_predicted = []
    for i,val in enumerate(x_test):
        y_predicted.append(np.dot(i,weights))
    return y_predicted

def convert_pred_to_binary(y_predicted):
    y_pred_binary = []
    for i in y_predicted:
        if i<0:
            y_pred_binary.append(0)
        else:
            y_pred_binary.append(1)
    return y_pred_binary
if __name__=="__main__":
    X,y = make_classification(n_samples=100000)
    X_train,X_test = X[:80000],X[80001:]
    y_train,y_test = y[:80000],y[80001:]
    sc = pyspark.SparkContext().getOrCreate()
    data = np.hstack([X_train, y_train.reshape(-1,1)]).tolist()
    updated_weights = train(sc,data)
    y_pred = test(X_test,updated_weights)
    y_pred_bin = convert_pred_to_binary(y_pred)
    acc_score = accuracy_score(y_test, y_pred_bin)
    print(acc_score)


