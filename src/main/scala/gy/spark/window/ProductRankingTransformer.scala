package gy.spark.window

import java.util.UUID

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}

class ProductRankingTransformer(spark: SparkSession) {

  def transform(eventsDF: DataFrame): DataFrame = {

    import org.apache.spark.sql.functions._
    import spark.implicits._

    val randUid = udf(() => UUID.randomUUID().toString.substring(0, 5))

    val userWindow = Window.partitionBy("userId").orderBy("eventTime")
    val df = eventsDF
      .withColumn("prevProduct", lag("product", 1).over(userWindow))

    val userProductWindow = Window.partitionBy("userId", "product").orderBy("eventTime")
    val sessionWindow = Window.partitionBy("sessionId")

    val sessionDF = df.orderBy("category", "userId", "eventTime")
      .withColumn("isNewSession", when(col("product") === col("prevProduct"), lit(0)).otherwise(lit(1)))
      .drop("prevProduct")
      .withColumn("randId", randUid())
      .orderBy(col("userId"), col("eventTime"), col("isNewSession").desc)
      .withColumn("sessionId", when(col("isNewSession") === lit(1), col("randId")).otherwise(skewness("randId").over(userProductWindow)))
//      .drop("randId", "isNewSession")
//      .withColumn("sessionStartTime", min("eventTime").over(sessionWindow))
//      .withColumn("sessionEndTime", max("eventTime").over(sessionWindow))
//      .withColumn("sessionDurationSec", unix_timestamp(col("sessionEndTime")) - unix_timestamp(col("sessionStartTime")))
//      .drop("sessionStartTime", "sessionEndTime")
//      .orderBy("category", "userId", "eventTime")
    sessionDF
//    val sumDurationDF = sessionDF
//      .select("category", "product", "userId", "sessionId", "sessionDurationSec")
//      .distinct()
//
//    sumDurationDF
  }

}
