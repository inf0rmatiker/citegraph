package org.citegraph.analytics

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

class Analytics(sparkSession: SparkSession, citationsDF: DataFrame, publishedDatesDF: DataFrame) {


  def findDensitiesByYear(): Unit = {

    // Find node density by year - group by year and count records
    publishedDatesDF.groupBy(col("year"))
      .count()
      .show(10)
  }



}
