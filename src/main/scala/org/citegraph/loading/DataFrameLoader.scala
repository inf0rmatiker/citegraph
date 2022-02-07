package org.citegraph.loading

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.citegraph.schemas.{citationsSchema, publishedDatesSchema}

class DataFrameLoader(val dataDirectory: String, val sparkSession: SparkSession) {

  // TODO: Fix these lines
//  val CITATIONS_TXT_STR : String = "citations.txt"
  val CITATIONS_TXT_STR : String = "citations-test.txt"
  val PUBLISHED_DATES_TXT_STR : String = "published-dates.txt"

  /**
   * Loads the citations.txt as a DataFrame, with "from" and "to" columns
   * @return A Spark SQL Dataframe like the following:
      +-------+-------+
      |   from|     to|
      +-------+-------+
      |9907233|9301253|
      |9907233|9504304|
      |9907233|9505235|
      |9907233|9506257|
      |9907233|9606402|
      |9907233|9607354|
      |...    |...    |
      +-------+-------+
   */
  def loadCitations(): DataFrame = {
    val sparkContext: SparkContext = sparkSession.sparkContext
    val citationsRDD: RDD[Row] = sparkContext.textFile(s"$dataDirectory/$CITATIONS_TXT_STR")
      .filter(line => !line.contains("#") && line.trim().nonEmpty) // Remove lines that contain '#' and empty lines
      .map(line => {
        val lineParts: Array[String] = line.split("\\s+") // Split on whitespace
        // Question: Does this remove lines with null values for from or to column?
        Row(lineParts(0).trim().toInt, lineParts(1).trim().toInt) // Output Row[Integer, Integer]
      })

    sparkSession.createDataFrame(citationsRDD, citationsSchema)
  }

  /**
   * Loads the published-dates.txt as a DataFrame, with "id" and "year" columns
   * @return A Spark SQL Dataframe like the following:
      +-------+----+
      |     id|year|
      +-------+----+
      |9203201|1992|
      |9203202|1992|
      |9203203|1992|
      |9203204|1992|
      |9203205|1992|
      |9203206|1992|
      |...    |... |
      +-------+----+
   */
  def loadPublishedDates(): DataFrame = {
    val sparkContext: SparkContext = sparkSession.sparkContext
    val publishedDatesRDD: RDD[Row] = sparkContext.textFile(s"$dataDirectory/$PUBLISHED_DATES_TXT_STR")
      .filter(line => !line.contains("#") && line.trim().nonEmpty) // Remove lines that contain '#' and empty lines
      .map(line => {
        val lineParts: Array[String] = line.split("\\s+") // Split on whitespace
        // Question: Could we use df.select(year('dt').alias('year')) to do this work?
        val year: Int = lineParts(1).trim().substring(0, 4).toInt // Only take first 4 digits of yyyy-mm-dd

        // For cross-listed papers, which have id 11<true_id>, just take the true id
        var rawPublishedId: String = lineParts(0).trim()
        if (rawPublishedId.length == 9 && rawPublishedId.startsWith("11")) { //
          rawPublishedId = rawPublishedId.substring(2)
        }

        Row(rawPublishedId.toInt, year) // Output Row[Integer, Integer]
      })

    sparkSession.createDataFrame(publishedDatesRDD, publishedDatesSchema)
  }


}
