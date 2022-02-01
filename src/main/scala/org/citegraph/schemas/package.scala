package org.citegraph

import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructType}

package object schemas {

  val citationsSchema: StructType = new StructType()
    .add("from", IntegerType, nullable = false)
    .add("to", IntegerType, nullable = false)

}
