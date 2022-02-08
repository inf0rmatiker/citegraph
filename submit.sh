#!/bin/bash

function print_usage {
  echo -e "USAGE\n\tsubmit.sh <input_data_directory> <output_directory>"
  echo -e "\nEXAMPLE\n"
  echo -e "\tsubmit.sh hdfs://bismarck:30201/cs535/data hdfs://bismarck:30201/cs535/output\n"
}

if [[ $# -eq 2 ]]; then
  echo -e "Submitting Spark Job...\n"
  "${SPARK_HOME}"/bin/spark-submit \
    --class org.citegraph.Application \
    --master spark://bismarck:9079 \
    --deploy-mode cluster \
    --driver-memory 4g \
    --executor-memory 2g \
    --executor-cores 1 \
    target/scala-2.12/citegraph_2.12-0.1.jar "$1" "$2"
else
  print_usage
fi
