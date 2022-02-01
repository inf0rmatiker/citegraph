package org.citegraph.loading

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

class DataFrameLoader(val dataDirectory: String, val sparkSession: SparkSession) {

  val CITATIONS_TXT_STR : String = "citations.txt"
  val PUBLISHED_DATES_TXT_STR : String = "published-dates.txt"

  /**
   * Loads the citations.txt as a DataFrame, with "from" and "to" columns
   * @return A Spark SQL Dataframe
   */
  def loadCitations(): DataFrame = {
    val sparkContext: SparkContext = sparkSession.sparkContext
    val citationsRDD = sparkContext.textFile(s"$dataDirectory/$CITATIONS_TXT_STR")
      .filter(line => !line.contains("#") && line.trim().nonEmpty) // Remove lines that contain '#' and empty lines
      .map(line => {
        val lineParts: Array[String] = line.split("\\s+") // Split on whitespace
        (lineParts(0).trim(), lineParts(1).trim())
      })

    sparkSession.createDataFrame(citationsRDD).toDF("from", "to")
  }


}
