#!/bin/bash

function print_usage {
  echo -e "USAGE\n\tsubmit.sh <cluster_manager> <input_data_directory> <output_directory>"
  echo -e "\nEXAMPLES\n"
  echo -e "\tsubmit.sh yarn hdfs://bismarck:30201/cs535/data hdfs://bismarck:30201/cs535/output\n"
  echo -e "\tsubmit.sh local[*] hdfs://bismarck:30201/cs535/data hdfs://bismarck:30201/cs535/output\n"
  echo -e "\tsubmit.sh spark://bismarck:30216 hdfs://bismarck:30201/cs535/data hdfs://bismarck:30201/cs535/output\n"
}

if [[ $# -eq 3 ]]; then
  echo -e "Submitting Spark Job...\n"
  HADOOP_CONF_DIR="$HOME/hadoop" "${SPARK_HOME}"/bin/spark-submit \
    --class org.citegraph.Application \
    --master "$1" \
    --deploy-mode cluster \
    --driver-memory 4g \
    --executor-memory 2g \
    --executor-cores 1 \
    target/scala-2.12/citegraph_2.12-0.1.jar "$1" "$2"
else
  print_usage
fi