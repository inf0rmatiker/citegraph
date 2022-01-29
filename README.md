# citegraph

Explorative Analytics of an Evolving Citation Network using Apache Spark. This project is the result of a team effort
on CS535: Big Data's Assignment 1 project.

_Authors_: [Caleb Carlson](https://github.com/inf0rmatiker), Nick Fabrizio

Project is written in Scala, and uses Apache Spark for data processing and Hadoop HDFS for distributed data storage.

## Usage

This project is intended to be submitted to a Spark Master via `spark-submit` as a packaged JAR.

- To build, compile, and package the project into a JAR, run `./refresh.sh`
- To submit the built JAR to a Spark Master, edit the Spark job config to your liking and run
  - `./submit.sh <hdfs_input_dir> <hdfs_output_dir>`
- To ingest the raw data into HDFS, use 
  - `./ingest.sh <input_data_directory> <hdfs_data_directory>`

## Docs

View the project assignment description under the [docs/](docs) directory