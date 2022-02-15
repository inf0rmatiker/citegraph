#!/bin/bash

function print_usage {
  echo -e "USAGE\n\tsubmit.sh <spark_master_uri> <citegraph_task_to_run> <input_data_directory> <output_directory>"
  echo -e "\nEXAMPLE\n"
  echo -e "\tsubmit.sh spark://bismarck:9079 density hdfs://bismarck:30201/cs535/data hdfs://bismarck:30201/cs535/output\n"
}

if [[ $# -eq 4 ]]; then
  SPARK_MASTER_URI=$1
  TASK=$2
  HDFS_INPUT_DIR=$3
  HDFS_OUTPUT_DIR=$4

  echo -e "Submitting Spark Job to $SPARK_MASTER_URI...\n"
  "${SPARK_HOME}"/bin/spark-submit \
    --class org.citegraph.Application \
    --master "$SPARK_MASTER_URI" \
    --deploy-mode cluster \
    target/scala-2.12/citegraph_2.12-0.1.jar "$TASK" "$HDFS_INPUT_DIR" "$HDFS_OUTPUT_DIR"
else
  print_usage
fi
