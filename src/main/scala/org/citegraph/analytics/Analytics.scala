package org.citegraph.analytics

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, sum}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

class Analytics(sparkSession: SparkSession, citationsDF: DataFrame, publishedDatesDF: DataFrame) {

  /**
   * Calculate running totals of densities
   */
  def calculateRunningTotals(orderCol: String, sumCol: String, df: DataFrame): DataFrame = {
    val previousRowsWindow = Window.orderBy(orderCol).rowsBetween(Window.unboundedPreceding, Window.currentRow)
    df.withColumn(sumCol, sum(sumCol) over previousRowsWindow)
  }

  /**
   * Use from IDs to join to publishedDatesDF and match edge counts to years
   */
  def getEdgeCountsByYear(df: DataFrame): DataFrame = {

    /*
    Get edge counts by year - group by year and count records with running total:
      +----+------+
      |year|  e(t)|
      +----+------+
      |1992|   170|
      |1993|  2919|
      |1994| 11568|
      |... |...   |
      +----+------+
     */

    publishedDatesDF
      .join(df, publishedDatesDF("id") === df("from"))
      .select("year", "count")
      .groupBy("year")
      .sum("count")
      .sort(col("year"))
      //      .withColumnRenamed("year", "edgeYear")
      .withColumnRenamed("sum(count)", "e(t)")
  }

  /**
   * Group by year, count and rename columns
   */
  def getNodeCountsByYear(): DataFrame = {

    /*
    Get node counts by year - group by year and count records with running total:
      +----+-----+
      |year| n(t)|
      +----+-----+
      |1993| 2852|
      |1994| 5746|
      |1995| 9190|
      |... |...  |
      +----+-----+
     */

    publishedDatesDF
      .groupBy(col("year"))
      .count()
      .withColumnRenamed("count", "n(t)")
      .withColumnRenamed("year", "nodeYear")
  }

  /**
   * Finds the density of both nodes n(t) and edges e(t) for each year t.
   */
  def findDensitiesByYear(): DataFrame = {

    /*
    Find node and edge density by year - group by year and count records with running total:
      +----+-----+------+
      |year| n(t)|  e(t)|
      +----+-----+------+
      |1992|  855|   170|
      |1993| 2852|  2919|
      |1994| 5746| 11568|
      |1995| 9190| 30161|
      |... |...  |...   |
      +----+-----+------+
     */

    val edgeCountsDF = citationsDF.groupBy(col("from")).count()

    val edgesByYearDF = getEdgeCountsByYear(edgeCountsDF)

    val runningTotalEdgeCounts = calculateRunningTotals("year", "e(t)", edgesByYearDF)

    val nodeCounts = getNodeCountsByYear()
    val runningTotalNodeCounts = calculateRunningTotals("nodeYear", "n(t)", nodeCounts)

    runningTotalNodeCounts
      .join(runningTotalEdgeCounts, runningTotalNodeCounts("nodeYear") === runningTotalEdgeCounts("year"))
      .select("year", "n(t)", "e(t)")
  }

}
