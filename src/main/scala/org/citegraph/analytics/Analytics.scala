package org.citegraph.analytics

import org.apache.spark.rdd.RDD
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

  /**
   * Copies citations in reverse order and creates a new dataframe with original and reversed citations
   */
  def generateBidirectionalGraph(): DataFrame = {

    val swappedColsCitationsDF = citationsDF
      .withColumn("to-new", citationsDF("from"))
      .drop("from")
      .withColumnRenamed("to", "from")
      .withColumnRenamed("to-new", "to")

    citationsDF.union(swappedColsCitationsDF)
  }

  /**
   * Creates a new dataframe with list of each node and all of its direct neighbors
   */
  def generateNodeNeighborsList(nodeList: DataFrame): Array[(Any, List[Any])] = {

    val neighborList = nodeList.rdd.map(row => {
      (row(0), List(row(1)))
    })

    neighborList.reduceByKey(_ ::: _).collect()
  }

  /**
   * Finds the effective diameter of the graph for each year t.
   */
  def findDiametersByYear(): DataFrame = {
    // X 1. Create bi-directional version of citations
    // X 2. Generate neighboring nodes list for each node
    // 3. Generate list of paths with length = 2
    // 4. Remove duplicate paths and paths that are not the shortest (i.e., where a shorter path exists between start and end nodes) from #3
    //    - For each path length list, create pairRDD with key = source~destination and value = path and sort it by key
    //    - Use subtractByKey between path length list and the path length list with one less edge (e.g., between path = 1 and path =2)

    val bidirectionalCitationsDF = generateBidirectionalGraph()
    //    return bidirectionalCitationsDF

    val nodeNeighbors = generateNodeNeighborsList(bidirectionalCitationsDF)
    //    nodeNeighbors.foreach(row => {
    //      println(row)
    //    })
    //    return nodeNeighbors
    return bidirectionalCitationsDF

  }

  def findNodePairsConnectedByNEdges(edgeCount: Int, year: Int): DataFrame = {
    citationsDF
  }

}
