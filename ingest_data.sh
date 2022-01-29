#!/bin/bash

function print_usage {
  echo -e "USAGE\n\ingest_data.sh <input_data_directory> <hdfs_data_directory>"
  echo -e "\nEXAMPLE\n\ingest_data.sh data hdfs://bismarck:30201/cs535/data\n"
}

[[ $# -ne 2 ]] && print_usage && exit 1
INPUT_DATA_DIR=$1
HDFS_DATA_DIR=$2

[[ -z $HADOOP_HOME ]] && echo "HADOOP_HOME not set. Please set HADOOP_HOME before continuing" && exit 1

HADOOP_DFS_BIN="$HADOOP_HOME/bin/hdfs dfs"
$HADOOP_DFS_BIN -mkdir "$HDFS_DATA_DIR"

cd "$INPUT_DATA_DIR" || exit 1
for FILE in ./*; do
  $HADOOP_DFS_BIN -copyFromLocal "$FILE" "$HDFS_DATA_DIR"
done