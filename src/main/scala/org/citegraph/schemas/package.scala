package org.citegraph

import org.apache.spark.sql.types.{IntegerType, StructType}

package object schemas {

  val citationsSchema: StructType = new StructType()
    .add("from", IntegerType, nullable = false)
    .add("to", IntegerType, nullable = false)

}
