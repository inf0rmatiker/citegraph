package org.citegraph

import org.apache.spark.rdd.RDD
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

  def loadTotalNodePairsFromCSV(sparkSession: SparkSession): Array[(Int, Long)] = {
    val rddFromFile: RDD[String] = sparkSession.sparkContext.textFile("hdfs://ant:30201/cs535/data/nodepairs.csv")
    rddFromFile.map(f=>{
      val parts = f.split(",")
      (parts(0).toInt, parts(1).toLong)
    }).collect()
  }

  def isValidHdfsUri(uri: String): Boolean = {
    uri.startsWith("hdfs://")
  }

  def main(args: Array[String]): Unit = {
    printArgs(args)

    var inputDirectory: String = ""
    var outputDirectory: String = ""
    var taskToRun: String = ""
    var isTestingEnv: Boolean = false

    // Parse input args
    if (args.length < 2 || args.length > 4) {
      printUsage()
      System.exit(1)
    } else if (args.length == 4 && args(0) == "--testing") {
      isTestingEnv = true
      printf("Running in local testing mode")
      taskToRun = args(1)
      inputDirectory = args(2)
      outputDirectory = args(3)
    } else if (args.length == 3) {
      taskToRun = args(0)
      inputDirectory = args(1)
      outputDirectory = args(2)
      if (!isValidHdfsUri(inputDirectory)) {
        printf("Invalid HDFS input directory: %s\n", inputDirectory)
        System.exit(1)
      } else if (!isValidHdfsUri(outputDirectory)) {
        printf("Invalid HDFS output directory: %s\n", outputDirectory)
        System.exit(1)
      } else if ((!taskToRun.equals("density")) && (!taskToRun.equals("diameter"))) {
        printf("Invalid task definition: %s\n", taskToRun)
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

    if (taskToRun == "density") {
      val densities: DataFrame = analytics.findDensitiesByYear()

      // Save DataFrames as .csv files to HDFS output directory
      val dataframeSaver: DataFrameSaver = new DataFrameSaver(outputDirectory)
      dataframeSaver.saveSortedAsCsv(filename = "densities", densities, sortByCol = "year")
    }

    if (taskToRun == "diameter") {
      val nodePairs: Array[(Int, Long)] = loadTotalNodePairsFromCSV(sparkSession)
      for (nodePairYear: (Int, Long) <- nodePairs) {
        val year: Int = nodePairYear._1
        val totalPairs: Long = nodePairYear._2
        val tableByYear: List[(Int, Long, Double)] = analytics.findGraphDiameterByYear(
          year,
          totalPairs,
          debug = isTestingEnv
        )

        print("tableByYear:\n")
        tableByYear.foreach {
          println
        }

        val resultsRDD = sparkSession.sparkContext.parallelize(tableByYear)
        val resultsDF = sparkSession.createDataFrame(resultsRDD).toDF("d", "g(d)", "percent_of_total")

        // Save DataFrames as .csv files to HDFS output directory
        val dataframeSaver: DataFrameSaver = new DataFrameSaver(outputDirectory)
        dataframeSaver.saveSortedAsCsv(filename = s"diameter_$year", resultsDF, sortByCol = "d")
      }
    }

    sparkSession.close()
  }

}
