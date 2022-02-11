#!/bin/bash

function print_usage {
  echo -e "USAGE\n\run_locally.sh"
}

if [[ $# -eq 2 ]]; then

  INPUT_DIR=$1
  OUTPUT_DIR=$2

  echo -e "Submitting Spark Job to local master...\n"
  "${SPARK_HOME}"/bin/spark-submit \
    --class org.citegraph.Application \
    --master "local[*]" \
    target/scala-2.12/citegraph_2.12-0.1.jar "--testing" "$INPUT_DIR" "$OUTPUT_DIR"
else
  print_usage
fi
