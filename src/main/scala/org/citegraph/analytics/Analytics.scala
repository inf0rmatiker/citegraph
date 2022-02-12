package org.citegraph.analytics

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, sum}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.{ListBuffer, Map}

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

  def findGraphDiameterByYear(edgeCount: Int, year: Int): DataFrame = {

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

    /*
     Filter all the edge pairs by only allowing pairs where both the "from" and the "to" ids are
     equal to or less than the given "year" parameter:
      +-------+-------+--------+------+
      |   from|     to|fromYear|toYear|
      +-------+-------+--------+------+
      |9409356|9301254|    1994|  1993|
      |9502280|9301254|    1995|  1993|
      |9404228|9301254|    1994|  1993|
      |9407347|9301254|    1994|  1993|
      |    ...|    ...|     ...|   ...|
      +-------+-------+--------+------+
     */
    val filteredByYearDF: DataFrame = bidirectionalEdgesDF.join(
      publishedDatesDF,
      bidirectionalEdgesDF("from") === publishedDatesDF("id")
    ).drop(col("id"))
      .withColumnRenamed(existingName = "year", newName = "fromYear")
      .filter($"fromYear" <= year)
      .join(
        publishedDatesDF,
        bidirectionalEdgesDF("to") === publishedDatesDF("id")
      ).drop(col("id"))
      .withColumnRenamed(existingName = "year", newName = "toYear")
      .filter($"toYear" <= year)
      .drop("fromYear", "toYear")

    val shortestPathsOfLengthOne: RDD[(String, Array[Int])] = filteredByYearDF.map(row => {
      val from: Int = row.getInt(0)
      val to: Int = row.getInt(1)
      val key: String = "%d~%d".format(from, to)
      val value: Array[Int] = Array(from, to)
      (key, value)
    }).rdd

    // Collect to array of rows
//    val shortestPathsOneArray: Array[Row] = shortestPathsOfLengthOne.collect()
//    val shortestPathsMap: mutable.Map[String, Array[Int]] = mutable.Map()
//    for (row: Row <- shortestPathsOneArray)
//      shortestPathsMap += (row.getString(0) -> row.getAs[Array[Int]](1))
//
//    shortestPathsMap.foreach{ i =>
//      printf("%s -> %s\n", i._1, i._2)
//    }

    collectAndPrintPairRDD(shortestPathsOfLengthOne, "shortestPathsOfLengthOne")

    /*
     Creates an id -> [adjacency list] mapping for nodes 1 edge away.
      +-------+--------------------+
      |     id|           neighbors|
      +-------+--------------------+
      |9501400|[9401341, 9210224...|
      |9405400|[9511399, 9510283...|
      |9509400|[9305287, 9302302...|
      |9403400|           [9509378]|
      |9406400|  [9410338, 9310210]|
      |9504400|[9209277, 9410240...|
      |9502400|[9505210, 9508402...|
      |9407400|  [9403328, 9501246]|
      |9512400|[9311222, 9501394...|
      |    ...|                 ...|
      +-------+--------------------+
     */
    val adjacencyListDF: DataFrame = filteredByYearDF.drop("fromYear", "toYear")  // Drop the year cols from above
      .map(row => {  // Convert all the "to" values to a Scala List containing the "to" value
        (row.getInt(0), List(row.getInt(1)))
      }).rdd.reduceByKey((a: List[Int], b: List[Int]) => {  // Convert to rdd so we can use reduceByKey API
        a ::: b  // Merge all the Lists sharing the same "from" key ( ":::" is a Scala List merge operator )
      }).toDF("id", "neighbors")  // Convert back to DataFrame with new column titles

//    val pathsOfLengthTwo: RDD[(String, Array[Int])] = adjacencyListDF.flatMap(row => {
//      val id: Int = row.getInt(0)
//      val neighbors: List[Int] = row.getAs[List[Int]](1)
//      val edges: ListBuffer[String] = ListBuffer[String]()
//      if (neighbors.length > 1) {
//        for (i: Int <- 0 to (neighbors.length-2)) {
//          for (j: Int <- (i + 1).until(neighbors.length)) {
//            var start: Int = neighbors(i)
//            var end: Int = neighbors(j)
//
//            // Swap if end < start
//            if (end < start) { val temp = end; end = start; start = temp }
//            edges += s"$start~$end:$start,$id,$end"
//          }
//        }
//      }
//      edges.toList
//    })
//    .map(encodedString => {
//      val parts: Array[String] = encodedString.split(":")
//      val endpoints: String = parts(0)
//      val path: Array[Int] = parts(1).split(",").map(_.toInt)
//      (endpoints, path)
//    }).rdd

//    val collectedPathsOfLengthTwo: Array[(String, Array[Int])] = pathsOfLengthTwo.collect()
//    print(collectedPathsOfLengthTwo.mkString("Array(",",",")"))

//    val subtracted: RDD[(String, Array[Int])] = shortestPathsOfLengthOne.subtractByKey(pathsOfLengthTwo)
//    val collectedSubtracted: Array[(String, Array[Int])] = subtracted.collect()
//    print(collectedSubtracted.mkString("Array(",",",")"))

    bidirectionalEdgesDF
  }

  def collectAndPrintPairRDD(pairRDD: RDD[(String, Array[Int])], name: String): Unit = {
    val collected: Array[(String, Array[Int])] = pairRDD.collect()
    val sb: mutable.StringBuilder = mutable.StringBuilder.newBuilder
    sb.append(s"PairRDD[(String, Array[Int])] $name:\n")
    collected.foreach{ x =>
      val arrayStr: String = x._2.mkString("Array(",",",")")
      sb.append(s"\t(\"$name\", $arrayStr)\n")
    }
    println(sb.toString())
  }

}
