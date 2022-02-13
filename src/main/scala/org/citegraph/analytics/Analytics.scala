package org.citegraph.analytics

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, sum}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.Map
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

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

  def findGraphDiameterByYear(year: Int, totalPairs: Long, debug: Boolean = false): List[(Int, Long, Double)] = {

    import sparkSession.implicits._

    println(s"totalPairs: $totalPairs")

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
//    val bidirectionalEdgesDF: DataFrame = citationsDF
//      .alias("invertedCitationsDF")  // Make a copy of citationsDF
//      .map(row => {
//        val from_original: Int = row.getInt(0)
//        val to_original: Int = row.getInt(1)
//        (to_original, from_original)  // Flip the (from, to) -> (to, from)
//      }).toDF("from", "to")  // Convert to Dataset (ignore the toDF function name)
//      .union(citationsDF)  // Union back with the original citations DF

    // citationsDF size: 421,578, bidirectionalEdgesDF size: 843,156
    //printf("citationsDF size: %,d, bidirectionalEdgesDF size: %,d\n", citationsDF.count(), bidirectionalEdgesDF.count())
    println("citationsDF.count " + citationsDF.count())
    val bidirectionalFilteredByYear: RDD[(Int, Int)] = citationsDF
      .join(
        publishedDatesDF,
        citationsDF("from") === publishedDatesDF("id")
      )
      .drop(col("id"))
      .withColumnRenamed(existingName = "year", newName = "fromYear")
      .join(
        publishedDatesDF,
        citationsDF("to") === publishedDatesDF("id")
      )
      .drop(col("id"))
      .withColumnRenamed(existingName = "year", newName = "toYear")
      .filter($"fromYear" <= year && $"toYear" <= year)
      .drop("fromYear", "toYear")
      .flatMap(row => {
        List((row.getInt(0), row.getInt(1)), (row.getInt(1), row.getInt(0)))
      }).rdd

    if (debug) {
      println("RDD[(Int, Int)] bidirectionalFilteredByYear:")
      bidirectionalFilteredByYear.collect().foreach{println}
      println()
    }


    /*
     Creates an id -> [adjacency list] mapping for nodes 1 edge away.
     */
    val adjacencyList: RDD[(Int, Array[Int])] = bidirectionalFilteredByYear
      .map(row => {  // Convert all the "to" values to a Scala List containing the "to" value
        (row._1, List(row._2))
      }).reduceByKey((a: List[Int], b: List[Int]) => {  // Convert to rdd so we can use reduceByKey API
      a ::: b  // Merge all the Lists sharing the same "from" key ( ":::" is a Scala List merge operator )
    }).map(row => {
      (row._1, row._2.toArray) // Convert value from List[Int] to Array[Int]
    }).sortByKey(ascending = true)

    collectAndPrintMapRDD(adjacencyList, "adjacencyMap")
    val adjacencyMap: Map[Int, Array[Int]] = adjacencyList.collectAsMap()

    val bidirectionalPathsOfLengthOne: RDD[((Int, Int), Array[Int])] = bidirectionalFilteredByYear.map(row => {
      ((row._1, row._2), Array(row._1, row._2))
    }).sortByKey(ascending = true)

    if (debug) collectAndPrintPairRDD(bidirectionalPathsOfLengthOne, "bidirectionalPathsOfLengthOne")


    val pathsOfLengthTwo: RDD[((Int, Int), Array[Int])] = adjacencyList.flatMap(row => {
      val id: Int = row._1
      val neighbors: Array[Int] = row._2
      val edges: ListBuffer[((Int, Int), Array[Int])] = ListBuffer[((Int, Int), Array[Int])]()
      if (neighbors.length > 1) {
        for (i: Int <- 0 to (neighbors.length-2)) {
          for (j: Int <- (i + 1).until(neighbors.length)) {
            var start: Int = neighbors(i)
            var end: Int = neighbors(j)

            // Swap if end < start
            if (end < start) { val temp = end; end = start; start = temp }
            edges += (((start,end), Array(start,id,end)))
          }
        }
      }
      edges.toList
    }).reduceByKey((a: Array[Int], _: Array[Int]) => a)
      .sortByKey(ascending = true)

    if (debug) collectAndPrintPairRDD(pathsOfLengthTwo, "pathsOfLengthTwo")

    /*
      ((9, 11), [9, 8, 11])  --> (9,  [11, 8, 9])
                             --> (11, [9, 8, 11])
     */
    val combinationTwo: RDD[(Int, Array[Int])] = pathsOfLengthTwo.flatMap( row => {
      val path: Array[Int] = row._2
      val start: Int = path(0); val end: Int = path(path.length-1)
      // val originalPathString: String = "%d:%s".format(start, path.mkString(","))
      val backwardsPath: Array[Int] = new Array[Int](path.length)
      for (i <- path.length-1 to 0 by -1) {
        backwardsPath((path.length-1) - i) = path(i)
      }
      //val backwardsPathString: String = "%d:%s".format(end, backwardsPath.mkString(","))
      List((start, backwardsPath), (end, path))
    })
    if (debug) collectAndPrintMapRDD(combinationTwo, "combinationTwo")

    /*
      ((4, 9), [4, 9])  --> (4, [4, 9])
      ((9, 4), [9, 4])  --> (9, [9, 4])
     */
    val combinationOne: RDD[(Int, Array[Int])] = bidirectionalPathsOfLengthOne.map(row => (row._1._1, row._2))
    if (debug) collectAndPrintMapRDD(combinationOne, "combinationOne")

    val combinationThree: RDD[((Int, Int), Array[Int])] = combinationTwo.join(combinationOne).map(row => {
      val pivot: Int = row._1 // The point that we are joining the 2-path and 1-path on
      val toPath: Array[Int] = row._2._1 // The path that leads to the pivot point
      val fromPath: Array[Int] = row._2._2 // The path that leads away from the pivot point
      // Allocate array big enough for both paths, minus the duplicate pivot point

      val fullPath: Array[Int] = new Array[Int](toPath.length + fromPath.length - 1)
      for (i <- toPath.indices) {
        fullPath(i) = toPath(i)
      }

      for (i <- 1 until fromPath.length) {
        fullPath(toPath.length + (i-1)) = fromPath(i)
      }
      val start: Int = fullPath(0); val end: Int = fullPath(fullPath.length-1)
      ((start, end), fullPath)
    })
    if (debug) collectAndPrintPairRDD(combinationThree, "combinationThree")


    /*var subtractedAndDistinct: RDD[(String, Array[Int])] = pathsOfLengthTwo
      .subtractByKey(shortestPathsOfLengthOne)
      .union(shortestPathsOfLengthOne)
      .sortByKey(ascending = true)

    if (debug) collectAndPrintPairRDD(subtractedAndDistinct, "subtractedAndDistinct")

    val results: ListBuffer[(Int, Long, Double)] = ListBuffer[(Int, Long, Double)]()

    // Add on length 1
    val lengthOneCount: Long = shortestPathsOfLengthOne.count()
    val lengthOnePercent: Double = (lengthOneCount * 1.0) / (totalPairs * 1.0)
    results += ((1, lengthOneCount, lengthOnePercent))

    // Add on length 2
    val lengthTwoCount: Long = subtractedAndDistinct.count()
    val lengthTwoPercent: Double = (lengthTwoCount * 1.0) / (totalPairs * 1.0)
    results += ((2, lengthTwoCount, lengthTwoPercent))

    // Unpersist length 1/2 since they are no longer needed
    pathsOfLengthTwo.unpersist()
    shortestPathsOfLengthOne.unpersist()

    // Length 3 and up
    var pathLength: Int = 2
    var generatedNewPaths: Boolean = true
    var count: Long = 0

    while (generatedNewPaths && (count < totalPairs)) {
      pathLength += 1
      val previousCount: Long = subtractedAndDistinct.count()

      subtractedAndDistinct = generateNextShortestPaths(pathLength, subtractedAndDistinct, adjacencyMap)
        .subtractByKey(subtractedAndDistinct)
        .union(subtractedAndDistinct)
        .sortByKey(ascending = true)

      count = subtractedAndDistinct.count()
      val countPercentage: Double = (count * 1.0) / (totalPairs * 1.0)
      generatedNewPaths = if (previousCount == count) false else true
      results += ((pathLength, count, countPercentage))
    }

    println(s"Stopped at $pathLength path length")
    if (debug) collectAndPrintPairRDD(subtractedAndDistinct, "subtractedAndDistinct")*/
    // results.toList
    List()
  }

  // .filter(row => row._2.length == nextPathLength)
  def generateNextShortestPaths(nextPathLength: Int, currentShortestPaths: RDD[(String, Array[Int])],
                                adjacencyMap: Map[Int, Array[Int]]): RDD[(String, Array[Int])] = {
    currentShortestPaths.flatMap{
        case(endpoints: String, path: Array[Int]) =>
          var edges: ListBuffer[String] = ListBuffer()
          val firstElement: Int = path(0)
          val lastElement: Int = path(path.length-1)
          val firstElementNeighbors: Array[Int] = adjacencyMap(firstElement)
          val lastElementNeighbors: Array[Int] = adjacencyMap(lastElement)

          // Iterate over neighbors of first element and see if there's any not already in the path
          // If there are, prepend it to the path.
          for (neighbor: Int <- firstElementNeighbors) {
            if (!path.contains(neighbor)) {
              var start: Int = neighbor
              var end: Int = lastElement

              // Swap if end < start
              if (end < start) { val temp = end; end = start; start = temp }
              edges += "%d~%d:%d,%s".format(start, end, neighbor, path.mkString(","))
            }
          }

          // Iterate over neighbors of last element and see if there's any not already in the path.
          // If there are, append it to the path.
          for (neighbor: Int <- lastElementNeighbors) {
            if (!path.contains(neighbor)) {
              var start: Int = firstElement
              var end: Int = neighbor

              // Swap if end < start
              if (end < start) { val temp = end; end = start; start = temp }
              edges += "%d~%d:%s,%d".format(start, end, path.mkString(","), neighbor)
            }
          }

          edges.toList
      }.map(encodedString => {
        val parts: Array[String] = encodedString.split(":")
        val endpoints: String = parts(0)
        val path: Array[Int] = parts(1).split(",").map(_.toInt)
        (endpoints, path)
      }).reduceByKey((a: Array[Int], _: Array[Int]) => a)
  }

  def collectAndPrintPairRDD(pairRDD: RDD[((Int,Int), Array[Int])], name: String): Unit = {
    val collected: Array[((Int,Int), Array[Int])] = pairRDD.collect()
    val sb: mutable.StringBuilder = mutable.StringBuilder.newBuilder
    sb.append(s"PairRDD[((Int,Int)), Array[Int])] $name:\n")
    collected.foreach{ x =>
      val arrayStr: String = x._2.mkString("Array(",",",")")
      sb.append("\t(\"(%d,%d)\", %s)\n".format(x._1._1, x._1._2, arrayStr))
    }
    println(sb.toString())
  }

  def collectAndPrintMapRDD(mapRDD: RDD[(Int, Array[Int])], name: String): Unit = {
    val collectedMap: Map[Int, Array[Int]] = mapRDD.collectAsMap()
    val sb: mutable.StringBuilder = mutable.StringBuilder.newBuilder
    sb.append(s"RDD[(Int, Array[Int])] $name:\n")
    collectedMap.keys.foreach{ key =>
      sb.append("\t(%d, %s)\n".format(key, collectedMap(key).mkString("Array(",",",")")))
    }
    println(sb.toString())
  }

}
