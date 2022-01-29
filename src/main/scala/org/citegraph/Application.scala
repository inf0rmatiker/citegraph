package org.citegraph

object Application {

  def printArgs(args: Array[String]): Unit = {
    for (i <- args.indices) {
      val arg: String = args(i)
      printf("args[%d]: %s\n", i, arg)
    }
  }

  def printUsage(): Unit = {
    println("USAGE")
    println("\tBuild:\n\t\tsbt package")
    println("\tSubmit as JAR to Spark cluster:\n\t\t$SPARK_HOME/bin/spark-submit <submit_options> \\")
    println("\t\ttarget/scala-2.13/citegraph_2.13-0.1.jar <hdfs_file>")
    println("\t\trequires option csv_directory needs to have path to where the data csv files are")
    println()
  }

  def main(args: Array[String]): Unit = {
    printArgs(args)
    if (args.length != 2) {
      printUsage()
      System.exit(1)
    }

  }

}
