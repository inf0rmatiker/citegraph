package org.citegraph

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.citegraph.analytics.Analytics
import org.citegraph.loading.DataFrameLoader
import org.citegraph.saving.DataFrameSaver

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

  def isValidHdfsUri(uri: String): Boolean = {
    uri.startsWith("hdfs://")
  }

  def main(args: Array[String]): Unit = {
    printArgs(args)
    if (args.length != 2) {
      printUsage()
      System.exit(1)
    }

    // Validate and fix inputDirectory
    var inputDirectory: String = args(0)
    val outputDirectory: String = args(1)

    // TODO: Uncomment these lines
//    if (!isValidHdfsUri(inputDirectory)) {
//      printf("Invalid HDFS input directory: %s\n", inputDirectory)
//      System.exit(1)
//    } else if (!isValidHdfsUri(outputDirectory)) {
//      printf("Invalid HDFS output directory: %s\n", outputDirectory)
//      System.exit(1)
//    }
//
//    if (inputDirectory.endsWith("/")) { // Chop off trailing slash
//      inputDirectory = inputDirectory.substring(0, inputDirectory.length - 1)
//    }
    printf("inputDirectory: %s, outputDirectory: %s\n", inputDirectory, outputDirectory)

    // Make a SparkSession and SparkContext
    val sparkSession: SparkSession = SparkSession.builder
      // TODO: remove this line
      .master("local")
      .appName("CiteGraph")
      .getOrCreate()

    // Load in citations and publication dates as Spark DataFrames
    val dataframeLoader: DataFrameLoader = new DataFrameLoader(inputDirectory, sparkSession)
    val citationsDF: DataFrame = dataframeLoader.loadCitations()
    val publishedDatesDF: DataFrame = dataframeLoader.loadPublishedDates()

    // Launch graph analytics; capture DataFrames for results
    val analytics: Analytics = new Analytics(sparkSession, citationsDF, publishedDatesDF)
    // TODO: uncomment this line
//    val densities: DataFrame = analytics.findDensitiesByYear()

    // Get analytics on graph diameters
    val diameters = analytics.findDiametersByYear()
    // TODO: remove this line
    diameters.show()

    // TODO: Uncomment these lines
//    // Save DataFrames as .csv files to HDFS output directory
//    val dataframeSaver: DataFrameSaver = new DataFrameSaver(outputDirectory)
//    dataframeSaver.saveSortedAsCsv(filename = "densities.csv", densities, sortByCol = "year")

    sparkSession.close()
  }

}
