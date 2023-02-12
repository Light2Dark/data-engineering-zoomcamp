# Week 3 Extra Notes

Main Notes are in Notion

## Model Deployment

1. gcloud auth login
2. bq --project_id data-eng-first extract -m taxi_data.tip_model gs://taxi-data-dataeng/tip_model
3. mkdir -p tmp/model
4. gsutil cp -r gs://taxi-data-dataeng/tip_model ./tmp/model
5. mkdir -p serving_dir/tip_model/1
6. mv -r tmp/model/* ./serving_dir/tip_model/1
7. rm -rf tmp
8. docker pull tensorflow/serving
9. docker run -p 8501:8501 --mount type=bind,source=`pwd`/serving_dir/tip_model,target=/models/tip_model -e MODEL_NAME=tip_model -t tensorflow/serving &
10. curl \\
  -d '{"instances": [{"passenger_count":1, "trip_distance":12.2, "PULocationID":"193", "DOLocationID":"264", "payment_type":"2","fare_amount":20.4,"tolls_amount":0.0}]}' \\
  -X POST <http://localhost:8501/v1/models/tip_model:predict>
