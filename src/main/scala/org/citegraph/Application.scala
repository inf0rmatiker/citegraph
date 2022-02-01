package org.citegraph

import org.apache.spark.sql.SparkSession

object Application {

  def printArgs(args: Array[String]): Unit = {
    for (i <- args.indices) {
      val arg: String = args(i)
      printf("args[%d]: %s\n", i, arg)
    }
  }

  def printUsage(): Unit = {
    println("USAGE")
    println("\tBuild:\n\t\tsbt package")
    println("\tSubmit as JAR to Spark cluster:\n\t\t$SPARK_HOME/bin/spark-submit <submit_options> \\")
    println("\t\ttarget/scala-2.13/citegraph_2.13-0.1.jar <hdfs_input_dir> <hdfs_output_dir>")
    println()
  }

  def main(args: Array[String]): Unit = {
    printArgs(args)
    if (args.length != 2) {
      printUsage()
      System.exit(1)
    }

    // Make a Spark Session
    val sparkSession: SparkSession = SparkSession.builder
      .appName("CiteGraph")
      .getOrCreate()

    val inputDirectory: String = args(0)
    val outputDirectory: String = args(1)

    printf("inputDirectory: %s, outputDirectory: %s\n", inputDirectory, outputDirectory)

    sparkSession.close()
  }

}
