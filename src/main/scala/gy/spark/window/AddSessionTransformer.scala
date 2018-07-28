package gy.spark.window

import java.util.UUID

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}

class AddSessionTransformer(spark: SparkSession) {

  def transform(eventsDF: DataFrame): DataFrame = {

    import org.apache.spark.sql.functions._
    import spark.implicits._


    val userCategoryWindow = Window.partitionBy("category", "userId").orderBy("eventTime")
    val sessionWindow = Window.partitionBy("sessionId")

    val randUid = udf(() => UUID.randomUUID().toString.substring(0, 5))

    eventsDF
      .select(
        col("category"),
        col("product"),
        col("userId"),
        col("eventTime"),
        col("eventType"),
        lag("eventTime", 1).over(userCategoryWindow).as("prevEventTime")
      )
      .withColumn("isNewSession", when(unix_timestamp($"eventTime") - unix_timestamp($"prevEventTime") < 300, lit(0)).otherwise(lit(1)))
      .withColumn("randId", randUid())
      .withColumn("sessionId", when(col("isNewSession") === lit(1), col("randId")).otherwise(first("randId").over(userCategoryWindow)))
      .withColumn("sessionStartTime", min("eventTime").over(sessionWindow))
      .withColumn("sessionEndTime", max("eventTime").over(sessionWindow))
      .drop("isNewSession", "randId", "prevEventTime")
      .orderBy($"sessionStartTime", $"sessionId", $"eventTime")
  }
}
