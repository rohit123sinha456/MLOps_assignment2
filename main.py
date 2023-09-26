#collapse
import numpy as np
from sklearn.datasets import make_classification
import pyspark
from sklearn.metrics import accuracy_score

def incomplete_cholesky_factorization(matrix):
    # Computing Cholesky Factorization
    H = np.linalg.cholesky(matrix)
    return H

# Function to compute local weights (simplified linear relation)
def compute_weights(QtQ, lambda_):
    # Computing Weights
    w_i = np.dot(QtQ, lambda_)
    return w_i

def distributed_svm(data_rdd, lambda_):
    # Map each partition of data to compute H, then QtQ
    H_rdd = data_rdd.mapPartitions(lambda partition: [incomplete_cholesky_factorization(np.array(list(partition)))])
    QtQ_rdd = H_rdd.map(lambda H: np.dot(H.T, H))
    
    # Collect QtQ from all partitions to the driver
    QtQ_list = QtQ_rdd.collect()
    
    # Compute local weights for each partition
    local_weights = [compute_weights(QtQ, lambda_) for QtQ in QtQ_list]
    
    # Simplified Allreduce to a sum of local weights
    global_weight = sum(local_weights)
    
    return global_weight

# Function to approx weights 
def LM_weight_approx(data,w,n_iter):
    x = []
    y = []
    l_rate = 0.01
    # Computing the RDD to list for computing dot products
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

# Function to train the model
def train(sc,data):
    rdd = sc.parallelize(data).cache()
    print(f"Our RDD has {rdd.getNumPartitions()} partitions.")
    w = np.zeros(len(X[0]))
    n_iter = 300
    for iter in range(n_iter):
        rdd_gradient = rdd.mapPartitions(lambda data : LM_weight_approx(data,w,n_iter))
        lst = rdd_gradient.collect()
        sumws = 0
        for ws in lst:
            sumws += ws
        w = sumws/len(lst)
    return w
# Function to Test the Model
def test(x_test,weights):
    y_predicted = []
    for i in x_test:
        y_predicted.append(np.dot(i,weights))
    return y_predicted

# Step Function for predicted results
def step_function(y_predicted):
    y_pred_binary = []
    for i in y_predicted:
        if i<0:
            y_pred_binary.append(0)
        else:
            y_pred_binary.append(1)
    return y_pred_binary

# Main Function to Run the Program
if __name__=="__main__":
    X,y = make_classification(n_samples=1000000)
    X_train,X_test = X[:800000],X[800001:]
    y_train,y_test = y[:800000],y[800001:]
    sc = pyspark.SparkContext().getOrCreate()
    data = np.hstack([X_train, y_train.reshape(-1,1)]).tolist()
    updated_weights = train(sc,data)
    y_pred = test(X_test,updated_weights)
    y_pred_bin = step_function(y_pred)
    acc_score = accuracy_score(y_test, y_pred_bin)
    print(f" Dataset Size :- 1000000")
    print(f" Accuracy Score :- {acc_score}")


