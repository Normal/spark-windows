package gy.spark.window

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

class DataLoader(implicit spark: SparkSession) {

  val eventsSchema = StructType(Array(
    StructField("category", StringType, nullable = false),
    StructField("product", StringType, nullable = false),
    StructField("userId", StringType, nullable = false),
    StructField("eventTime", TimestampType, nullable = false),
    StructField("eventType", StringType, nullable = true)
  ))

  def load(file: String, delimiter: String, withHeader: Boolean): DataFrame = {
    spark.read.format("csv")
      .options(Map("header" -> withHeader.toString, "delimiter" -> delimiter, "quote" -> "\u0000"))
      .schema(eventsSchema)
      .load(file)
  }
}
