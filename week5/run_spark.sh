python spark_gcloud_local.py \
    --input_green gs://data-eng-first/spark/pq/green/*/* \
    --input_yellow gs://data-eng-first/spark/pq/yellow/*/*  \
    --output gs://data-eng-first/spark/report/revenue

URL="spark://data-eng-vm.asia-southeast1-a.c.data-eng-first.internal:7077"
spark-submit \
    --master="${URL}" \
    python spark_gcloud_local.py \
        --input_green data/pq/green/2021/* \
        --input_yellow data/pq/yellow/2021/*  \
        --output data/report/revenue-2021


# dataproc
python spark_gcloud_local.py \
    --input_green=gs://taxi-data-dataeng/spark/pq/green/2021/* \
    --input_yellow=gs://taxi-data-dataeng/spark/pq/yellow/2021/*  \
    --output=gs://taxi-data-dataeng/spark/report/revenue-2021