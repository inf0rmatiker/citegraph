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
    println("\t\ttarget/scala-2.13/citegraph_2.13-0.1.jar [--testing] <input_dir> <output_dir>")
    println()
  }

  def isValidHdfsUri(uri: String): Boolean = {
    uri.startsWith("hdfs://")
  }

  def main(args: Array[String]): Unit = {
    printArgs(args)

    var inputDirectory: String = ""
    var outputDirectory: String = ""
    var isTestingEnv: Boolean = false

    // Parse input args
    if (args.length < 2 || args.length > 3) {
      printUsage()
      System.exit(1)
    } else if (args.length == 3 && args(0) == "--testing") {
      isTestingEnv = true
      printf("Running in local testing mode")
      inputDirectory = args(1)
      outputDirectory = args(2)
    } else if (args.length == 2) {
      inputDirectory = args(0)
      outputDirectory = args(1)
      if (!isValidHdfsUri(inputDirectory)) {
        printf("Invalid HDFS input directory: %s\n", inputDirectory)
        System.exit(1)
      } else if (!isValidHdfsUri(outputDirectory)) {
        printf("Invalid HDFS output directory: %s\n", outputDirectory)
        System.exit(1)
      }
    }

    // Fix input directory
    if (inputDirectory.endsWith("/")) { // Chop off trailing slash
      inputDirectory = inputDirectory.substring(0, inputDirectory.length - 1)
    }
    printf("inputDirectory: %s, outputDirectory: %s\n", inputDirectory, outputDirectory)

    // Make a SparkSession and SparkContext
    val sparkSession: SparkSession = SparkSession.builder
      .appName("CiteGraph")
      .getOrCreate()

    // Load in citations and publication dates as Spark DataFrames
    val dataframeLoader: DataFrameLoader = new DataFrameLoader(inputDirectory, sparkSession)
    val citationsDF: DataFrame = dataframeLoader.loadCitations()
    val publishedDatesDF: DataFrame = dataframeLoader.loadPublishedDates()

    // Launch graph analytics; capture DataFrames for results
    val analytics: Analytics = new Analytics(sparkSession, citationsDF, publishedDatesDF)
    //val densities: DataFrame = analytics.findDensitiesByYear()
    val tableByYear: List[(Int, Long, Double)] = analytics.findGraphDiameterByYear(year = 1993, debug = isTestingEnv)
    print("tableByYear:\n")
    tableByYear.foreach{println}

    val resultsRDD = sparkSession.sparkContext.parallelize(tableByYear)
    val resultsDF = sparkSession.createDataFrame(resultsRDD).toDF("d", "g(d)", "percent_of_total")

    // Save DataFrames as .csv files to HDFS output directory
    val dataframeSaver: DataFrameSaver = new DataFrameSaver(outputDirectory)
    dataframeSaver.saveSortedAsCsv(filename = "diameter_1998.csv", resultsDF, sortByCol = "d")

    sparkSession.close()
  }

}
