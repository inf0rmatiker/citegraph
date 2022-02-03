package org.citegraph.saving

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

class DataFrameSaver(val outputDirectory: String) {

  def saveAsCsv(filename: String, df: DataFrame): Unit = {
    df.coalesce(1)
      .write
      .option("header","true")
      .option("sep",",")
      .mode("overwrite")
      .csv(s"$outputDirectory/$filename")
  }

  def saveSortedAsCsv(filename: String, df: DataFrame, sortByCol: String): Unit = {
    df.coalesce(1)
      .sort(col(sortByCol))
      .write
      .option("header","true")
      .option("sep",",")
      .mode("overwrite")
      .csv(s"$outputDirectory/$filename")
  }

}
