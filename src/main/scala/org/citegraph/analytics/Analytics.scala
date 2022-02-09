package org.citegraph.analytics

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, sum}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

class Analytics(sparkSession: SparkSession, citationsDF: DataFrame, publishedDatesDF: DataFrame) {


  /* -------------------------------------------------------------------------------------------
    Question 1 Functions: TODO: Remove this banner. This is just to temporarily help organize
   --------------------------------------------------------------------------------------------*/

  /**
   * Calculate running totals of densities
   */
  def calculateRunningTotals(orderCol: String, sumCol: String, df: DataFrame): DataFrame = {
    val previousRowsWindow = Window.orderBy(orderCol).rowsBetween(Window.unboundedPreceding, Window.currentRow)
    df.withColumn(sumCol, sum(sumCol) over previousRowsWindow)
  }

  /**
   * Use "from" IDs to join to publishedDatesDF and match edge counts to years, producing the following DataFrame:
      +----+------+
      |year|  e(t)|
      +----+------+
      |1992|   170|
      |1993|  2919|
      |1994| 11568|
      |... |...   |
      +----+------+
   */
  def getEdgeCountsByYear(df: DataFrame): DataFrame = {
    publishedDatesDF
      .join(df, publishedDatesDF("id") === df("from"))  // Inner join to publishedDates on the paper id
      .select("year", "count")  // Select only the "year" and "count" columns
      .groupBy("year").sum("count")  // Group by "year" and sum up the counts
      .sort(col("year"))  // Sort by "year"
      .withColumnRenamed("sum(count)", "e(t)")  // Rename "sum(count)" column to "e(t)"
  }

  /**
   * Gets the total node count for each given year, producing the following DataFrame:
      +----+-----+
      |year| n(t)|
      +----+-----+
      |1993| 2852|
      |1994| 5746|
      |1995| 9190|
      |... |...  |
      +----+-----+
   */
  def getNodeCountsByYear: DataFrame = {
    publishedDatesDF
      .groupBy(col("year")).count()  // Group by "year", taking the count()
      .withColumnRenamed("count", "n(t)")  // Rename "count" to "n(t)"
      .withColumnRenamed("year", "nodeYear")  // Rename "year" to "nodeYear"
  }

  /**
   * Finds the density of both nodes n(t) and edges e(t) for each year t, producing the following DataFrame:
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
  def findDensitiesByYear(): DataFrame = {

    /*
    Find node and edge density by year - group by year and count records with running total:
     */

    /*
      Group by the from column, using the aggregate count().
      +-------+-----+
      |   from|count|
      +-------+-----+
      |9801317|    4|
      |   8086|    6|
      |9510372|   15|
      |9310298|    1|
      |9710374|   14|
      |9606386|   36|
      |...    |...  |
      +-------+-----+
     */
    val edgeCountsDF: DataFrame = citationsDF.groupBy(col("from")).count()

    /*
      +----+------+
      |year|  e(t)|
      +----+------+
      |1992|   170|
      |1993|  2919|
      |1994| 11568|
      |... |...   |
      +----+------+
     */
    val edgesByYearDF: DataFrame = getEdgeCountsByYear(edgeCountsDF)

    /*
      +----+------+
      |year|  e(t)|
      +----+------+
      |1992|   170|
      |1993|  2919|
      |1994| 11568|
      |... |...   |
      +----+------+
     */
    val runningTotalEdgeCounts: DataFrame = calculateRunningTotals("year", "e(t)", edgesByYearDF)
    runningTotalEdgeCounts.show()

    val nodeCounts: DataFrame = getNodeCountsByYear
    val runningTotalNodeCounts = calculateRunningTotals("nodeYear", "n(t)", nodeCounts)

    /*
    Join node and edge count DataFrames together by their year:
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
    runningTotalNodeCounts
      .join(runningTotalEdgeCounts, runningTotalNodeCounts("nodeYear") === runningTotalEdgeCounts("year"))
      .select("year", "n(t)", "e(t)")
  }

  /* -------------------------------------------------------------------------------------------
    Question 2 Functions: TODO: Remove this banner. This is just to temporarily help organize
   --------------------------------------------------------------------------------------------*/

  def findNodePairsConnectedByNEdges(edgeCount: Int, year: Int): DataFrame = {

    import sparkSession.implicits._

    /*
    Create bi-directional edges for the citationsDF:
      +-------+-------+       +-------+-------+
      |   from|     to|       |   from|     to|
      +-------+-------+       +-------+-------+
      |      2|      1|       |      2|      1|
      |      3|      2|       |      1|      2|
      |      3|      1|  -->  |      3|      2|
      |    ...|    ...|       |      2|      3|
      +-------+-------+       |      3|      1|
                              |      1|      3|
                              |    ...|    ...|
                              +-------+-------+
     */
    val bidirectionalEdgesDF: DataFrame = citationsDF
      .alias("invertedCitationsDF")  // Make a copy of citationsDF
      .map(row => {
        val from_original: Int = row.getInt(0)
        val to_original: Int = row.getInt(1)
        (to_original, from_original)  // Flip the (from, to) -> (to, from)
      }).toDF("from", "to")  // Convert to Dataset (ignore the toDF function name)
      .union(citationsDF)  // Union back with the original citations DF

    // citationsDF size: 421,578, bidirectionalEdgesDF size: 843,156
    printf("citationsDF size: %,d, bidirectionalEdgesDF size: %,d\n", citationsDF.count(), bidirectionalEdgesDF.count())


    val filteredByYear: DataFrame = bidirectionalEdgesDF.join(
      publishedDatesDF,
      bidirectionalEdgesDF("from") === publishedDatesDF("id")
    ).drop(col("id"))
      .withColumnRenamed(existingName = "year", newName = "fromYear")
      .filter($"fromYear" <= year)
      .join(
        publishedDatesDF,
        bidirectionalEdgesDF("to") === publishedDatesDF("id")
      ).withColumnRenamed(existingName = "year", newName = "fromYear")

    filteredByYear.show()

    bidirectionalEdgesDF
  }

}
