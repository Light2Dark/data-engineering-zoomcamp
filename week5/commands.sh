wget https://download.java.net/java/GA/jdk19.0.2/fdb695a9d9064ad6b064dc6df578380c/7/GPL/openjdk-19.0.2_linux-x64_bin.tar.gz
tar xzfv openjdk-filename
rm openjdk-filename
export JAVA_HOME="${HOME}/spark/jdk-19.0.2"
export PATH="${JAVA_HOME}/bin:${PATH}"

wget https://dlcdn.apache.org/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3.tgz
tar xzfv spark-filename
rm spark-filename
export SPARK_HOME="${HOME}/spark/spark-3.3.2-bin-hadoop3"
export PATH="${SPARK_HOME}/bin:${PATH}"
# add to .bashrc and run source .bashrc

export PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH"
export PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.5-src.zip:$PYTHONPATH"

cd week5
wget https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv


# gsutil -m cp -r data/pq gs://taxi-data-dataeng/spark/
gsutil cp gs://hadoop-lib/gcs/gcs-connector-hadoop3-2.2.11.jar gcs-connector-hadoop3-2.2.11.jar