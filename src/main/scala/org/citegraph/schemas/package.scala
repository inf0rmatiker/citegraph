package org.citegraph

import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StructType}

package object schemas {

  val citationsSchema: StructType = new StructType()
    .add(name = "from", dataType = IntegerType, nullable = false)
    .add(name = "to",   dataType = IntegerType, nullable = false)

  val publishedDatesSchema: StructType = new StructType()
    .add(name = "id",   dataType = IntegerType, nullable = false)
    .add(name = "year", dataType = IntegerType, nullable = false)

  val outputSchema: StructType = new StructType()
    .add(name = "d",   dataType = IntegerType, nullable = false)
    .add(name = "g(d)", dataType = LongType, nullable = false)
    .add(name = "percent_of_total", dataType = DoubleType, nullable = false)

}
