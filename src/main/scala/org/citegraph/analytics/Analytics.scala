package org.citegraph.analytics

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

class Analytics(sparkSession: SparkSession, citationsDF: DataFrame, publishedDatesDF: DataFrame) {


  def findDensitiesByYear(): Unit = {

    // Find node density by year - group by year
    publishedDatesDF.groupBy(col("year"))
      .sum("id")
      .show(10)
  }



}
