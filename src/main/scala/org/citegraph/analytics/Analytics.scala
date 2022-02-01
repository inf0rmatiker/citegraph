package org.citegraph.analytics

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

class Analytics(sparkSession: SparkSession, citationsDF: DataFrame, publishedDatesDF: DataFrame) {


  def findDensitiesByYear(): Unit = {

    /*
    Find node density by year - group by year and count records:
      +----+-----+
      |year|count|
      +----+-----+
      |1997| 4252|
      |1994| 2894|
      |1996| 3939|
      |... |...  |
      +----+-----+
     */
    publishedDatesDF.groupBy(col("year"))
      .count()
      .show()


  }



}
