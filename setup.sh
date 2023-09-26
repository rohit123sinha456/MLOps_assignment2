
wget https://dlcdn.apache.org/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz
tar -xzf spark-3.5.0-bin-hadoop3.tgz
mv spark-3.5.0-bin-hadoop3 spark
export SPARKHOME=/home/labuser/spark
export PATH=$PATH:$SPARKHOME/bin
source ~/.bashrc
pip install pyspark numpy scikit-learn