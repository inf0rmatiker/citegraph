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

  /**
   * Finds the length (d) of the maximum shortest path needed to be calculated between nodes
   * to represent at least 90% of all possible node pairs in the graph.
   */
  def findGraphDiameterByYear(year: Int, totalPairs: Long, debug: Boolean = false): List[(Int, Long, Double)] = {

    println(s"totalPairs: $totalPairs")
    val results: ListBuffer[(Int, Long, Double)] = ListBuffer[(Int, Long, Double)]()
    val pathsOfLengthDRDDs: ListBuffer[RDD[((Int, Int), Array[Int])]] = ListBuffer[RDD[((Int, Int), Array[Int])]]()
    val combinedShortestPathsRDDs: ListBuffer[RDD[((Int, Int), Array[Int])]] = ListBuffer[RDD[((Int, Int), Array[Int])]]()

    val bidirectionalFilteredByYear: RDD[(Int, Int)] = getBidirectionalEdgesByYearRDD(
      citationsDF, publishedDatesDF, year, sparkSession
    )

    /*
     Creates an adjacency list mapping for nodes 1 edge away, i.e:
     (1, [<neighbors_1>]),
     (2, [<neighbors_2>]),
     ...
     (n, [<neighbors_n>])
     */
    val adjacencyListRDD: RDD[(Int, Array[Int])] = getAdjacencyListRDD(bidirectionalFilteredByYear)
    val adjacencyMap: Map[Int, Array[Int]] = adjacencyListRDD.collectAsMap()
    if (debug) collectAndPrintMapRDD(adjacencyListRDD, "adjacencyMap")

    // d = 1
    var d: Int = 1
    pathsOfLengthDRDDs += getPathsOfLengthOneRDD(bidirectionalFilteredByYear)
    if (debug) collectAndPrintPairRDD(pathsOfLengthDRDDs.head, "pathsOfLengthOne")
    recordResultsForD(d, totalPairs, pathsOfLengthDRDDs.last, results) // Add on d = 1 results

    d += 1  // d = 2
    pathsOfLengthDRDDs += getPathsOfLengthTwoRDD(adjacencyListRDD)
    if (debug) collectAndPrintPairRDD(pathsOfLengthDRDDs(d-1), "pathsOfLengthTwo")
    combinedShortestPathsRDDs += combineWithPathsOfLengthD(pathsOfLengthDRDDs.last, pathsOfLengthDRDDs.head)
    recordResultsForD(d, totalPairs, combinedShortestPathsRDDs.last, results) // Add on d = 2 results
    if (debug) collectAndPrintPairRDD(combinedShortestPathsRDDs.head, "combinedShortestPaths2")

    pathsOfLengthDRDDs.head.unpersist() // Unpersist head of list (paths of length 1)
    pathsOfLengthDRDDs.remove(0) // Chop off head of list, Garbage Collection will clean up now that references are gone

    // ------------ d = 3 and up ------------

    var generatedNewPaths: Boolean = true
    var count: Long = 0

    while (generatedNewPaths && (count < totalPairs) && (d <= 20)) {
      d += 1
      val previousCount: Long = combinedShortestPathsRDDs.last.count()

      // Calculate newest paths of length d, and combine with current shortest paths
      pathsOfLengthDRDDs += generatePathsOfLengthD(pathsOfLengthDRDDs.last, adjacencyMap)
      combinedShortestPathsRDDs += combineWithPathsOfLengthD(pathsOfLengthDRDDs.last, combinedShortestPathsRDDs.last)

      // Determine pair count, whether we should continue, and record results
      count = recordResultsForD(d, totalPairs, combinedShortestPathsRDDs.last, results)
      generatedNewPaths = if (previousCount == count) false else true

      // Unpersist obsolete RDDs and delete references
      pathsOfLengthDRDDs.head.unpersist()
      pathsOfLengthDRDDs.remove(0)
      combinedShortestPathsRDDs.head.unpersist()
      combinedShortestPathsRDDs.remove(0)
    }

    println(s"Stopped at $d path length")
    results.toList
  }

  /**
   * Uses the adjacency map and paths of length d-1 to generate paths of length d by adding to
   * the beginning or end of the original paths.
   */
  def generatePathsOfLengthD(pathsOfLengthDMinusOne: RDD[((Int, Int), Array[Int])],
                                adjacencyMap: Map[Int, Array[Int]]): RDD[((Int, Int), Array[Int])] = {
    pathsOfLengthDMinusOne.flatMap{
      case((start: Int, end: Int), path: Array[Int]) =>
        val newRows: ListBuffer[((Int, Int), Array[Int])] = new ListBuffer[((Int, Int), Array[Int])]()
        val firstElementNeighbors: Array[Int] = adjacencyMap(start)
        val lastElementNeighbors: Array[Int] = adjacencyMap(end)

        // Iterate over neighbors of first element and see if there's any not already in the path
        // If there are, prepend it to the path.
        for (neighbor: Int <- firstElementNeighbors) {
          if (!path.contains(neighbor)) {
            val newStart: Int = neighbor
            val newEnd: Int = end

            // Swap if end < start
            val newKey: (Int, Int) = if (newEnd < newStart) (newEnd, newStart) else (newStart, newEnd)

            // New array: [ <start_neighbor>, <original_path> ]
            val newPath: Array[Int] = new Array[Int](path.length + 1)
            newPath(0) = newStart
            for (i <- path.indices) newPath(i+1) = path(i)
            newRows += ((newKey, newPath))
          }
        }

        // Iterate over neighbors of last element and see if there's any not already in the path.
        // If there are, append it to the path.
        for (neighbor: Int <- lastElementNeighbors) {
          if (!path.contains(neighbor)) {
            val newStart: Int = start
            val newEnd: Int = neighbor

            // Swap if end < start
            val newKey: (Int, Int) = if (newEnd < newStart) (newEnd, newStart) else (newStart, newEnd)

            // New array: [ <original_path>, <end_neighbor> ]
            val newPath: Array[Int] = new Array[Int](path.length + 1)
            for (i <- path.indices) newPath(i) = path(i)
            newPath(newPath.length-1) = newEnd
            newRows += ((newKey, newPath))
          }
        }
        newRows.toList
    }.reduceByKey((a: Array[Int], _: Array[Int]) => a)
  }

  /**
   * Combines the distinct paths of length d with the current known shortest paths up to length d-1.
   */
  def combineWithPathsOfLengthD(pathsOfLengthD: RDD[((Int, Int), Array[Int])],
                                currentShortestPaths: RDD[((Int, Int), Array[Int])]): RDD[((Int, Int), Array[Int])] = {
    pathsOfLengthD.subtractByKey(currentShortestPaths).union(currentShortestPaths)
  }

  /**
   * Collects a PairRDD[((Int, Int), Array[Int])] and prints it in a readable format.
   * Don't use on massive RDDs.
   */
  def collectAndPrintPairRDD(pairRDD: RDD[((Int, Int), Array[Int])], name: String): Unit = {
    val collected: Array[((Int, Int), Array[Int])] = pairRDD.collect()
    val sb: mutable.StringBuilder = mutable.StringBuilder.newBuilder
    sb.append(s"PairRDD[((Int, Int), Array[Int])] $name:\n")
    collected.foreach{x =>
      val key = x._1
      val arrayStr: String = x._2.mkString("Array(",",",")")
      sb.append("\t((%d, %d)), %s)\n".format(key._1, key._2, arrayStr))
    }
    println(sb.toString())
  }

  /**
   * Collects a PairRDD[(Int, Array[Int])] and prints it in a readable format.
   * Don't use on massive RDDs.
   */
  def collectAndPrintMapRDD(mapRDD: RDD[(Int, Array[Int])], name: String): Unit = {
    val collectedMap: Map[Int, Array[Int]] = mapRDD.collectAsMap()
    val sb: mutable.StringBuilder = mutable.StringBuilder.newBuilder
    sb.append(s"RDD[(Int, Array[Int])] $name:\n")
    collectedMap.keys.foreach{ key =>
      sb.append("\t(%d, %s)\n".format(key, collectedMap(key).mkString("Array(",",",")")))
    }
    println(sb.toString())
  }

  /**
   * Calculates and returns the adjacent neighbors for every node in the form (id, [neighbors]) as an RDD.
   */
  def getAdjacencyListRDD(bidirectionalEdges: RDD[(Int, Int)]): RDD[(Int, Array[Int])] = {
    bidirectionalEdges.map(row => {  // Convert all the "to" values to a Scala List containing the "to" value
        (row._1, List(row._2))
      }).reduceByKey((a: List[Int], b: List[Int]) => {  // Convert to rdd so we can use reduceByKey API
      a ::: b  // Merge all the Lists sharing the same "from" key ( ":::" is a Scala List merge operator )
    }).map(row => {
      (row._1, row._2.toArray) // Convert value from List[Int] to Array[Int]
    }).sortByKey(ascending = true) // Sort key for ease of reading
  }

  /**
   * Creates and returns a bidirectional edges list, i.e:
   * (1, 2),
   * (2, 1),
   * (1, 3),
   * (3, 1),
   * ...
   * (n, m),
   * (m, n)
   * ...where both the from and to ids have to be less-than or equal to the year given.
   */
  def getBidirectionalEdgesByYearRDD(citationsDF: DataFrame, publishedDatesDF: DataFrame, year: Int,
                                     sparkSession: SparkSession): RDD[(Int, Int)] = {
    import sparkSession.implicits._

    citationsDF.join(
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
  }

  /**
   * Uses the bidirectional edges RDD to calculate all unique paths of length d = 1.
   */
  def getPathsOfLengthOneRDD(bidirectionalEdgesRDD: RDD[(Int, Int)]): RDD[((Int, Int), Array[Int])] = {
    bidirectionalEdgesRDD.map(row => {
      val start: Int = row._1; val end: Int = row._2
      val key: (Int, Int) = if (end < start) (end, start) else (start, end)
      val path: Array[Int] = Array(start, end)
      (key, path)
    }).reduceByKey((a: Array[Int], _: Array[Int]) => a, numPartitions = 16)
  }

  /**
   * Uses the adjacency list RDD to calculate all unique paths of length d = 2.
   */
  def getPathsOfLengthTwoRDD(adjacencyListRDD: RDD[(Int, Array[Int])]): RDD[((Int, Int), Array[Int])] = {
    adjacencyListRDD.flatMap(row => {
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
    }).reduceByKey((a: Array[Int], _: Array[Int]) => a, numPartitions = 16)
  }

  /**
   * Counts and records the total number of node-pair shortest paths found for a given maximum path
   * length d. Also records the percentage of the total number of node-pairs that was achieved.
   */
  def recordResultsForD(d: Int, totalPairs: Long, shortestPathsRDD: RDD[((Int, Int), Array[Int])],
                        results: ListBuffer[(Int, Long, Double)]): Long = {
    val pairCount: Long = shortestPathsRDD.count()
    val pairPercent: Double = (pairCount * 1.0) / (totalPairs * 1.0)
    results += ((d, pairCount, pairPercent))
    println(s"Result added: ($d, $pairCount, $pairPercent)")
    pairCount
  }

}
