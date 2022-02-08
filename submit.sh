#!/bin/bash

function print_usage {
  echo -e "USAGE\n\tsubmit.sh <spark_master_uri> <input_data_directory> <output_directory>"
  echo -e "\nEXAMPLE\n"
  echo -e "\tsubmit.sh spark://bismarck:9079 hdfs://bismarck:30201/cs535/data hdfs://bismarck:30201/cs535/output\n"
}

if [[ $# -eq 3 ]]; then
  SPARK_MASTER_URI=$1
  HDFS_INPUT_DIR=$2
  HDFS_OUTPUT_DIR=$3

  echo -e "Submitting Spark Job to $SPARK_MASTER_URI...\n"
  "${SPARK_HOME}"/bin/spark-submit \
    --class org.citegraph.Application \
    --master "$SPARK_MASTER_URI" \
    --deploy-mode cluster \
    --driver-memory 4g \
    --executor-memory 2g \
    --executor-cores 1 \
    target/scala-2.12/citegraph_2.12-0.1.jar "$HDFS_INPUT_DIR" "$HDFS_OUTPUT_DIR"
else
  print_usage
fi
