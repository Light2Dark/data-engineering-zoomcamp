# interrupts the script if any command returns a non-zero exit status
set -e

TAXI_TYPE=$1
YEAR=$2

# https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2019-01.csv.gz

for MONTH in {1..12}; do
    FMONTH=`printf "%02d" $MONTH`
    LINK="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/${TAXI_TYPE}/${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.csv.gz"

    LOCAL_PREFIX="data/raw/${TAXI_TYPE}/${YEAR}"
    LOCAL_FILENAME="${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.csv.gz"
    LOCAL_PATH="${LOCAL_PREFIX}/${LOCAL_FILENAME}"
    mkdir -p $LOCAL_PREFIX

    echo "Downloading $LINK to $LOCAL_PATH"
    wget $LINK -O $LOCAL_PATH

    # echo "Compressing $LOCAL_PATH"
    # gzip $LOCAL_PATH
done

# zcat data/yellow/2020/yellow_tripdata_2020-01.csv.gz | head -n 5
# tree data
# ls -lhR <folder_name>