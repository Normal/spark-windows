package gy.spark.window

import java.util.UUID

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}

class ProductRankingTransformer(spark: SparkSession) {

  def transform(eventsDF: DataFrame): DataFrame = {

    import org.apache.spark.sql.functions._
    import spark.implicits._

    val randUid = udf(() => UUID.randomUUID().toString.substring(0, 5))

    val userWindow = Window.partitionBy("userId").orderBy("eventTime")
    val userProductWindow = Window.partitionBy("userId", "product").orderBy("eventTime")
    val sessionWindow = Window.partitionBy("category", "userId", "product", "sessionId").orderBy("eventTime")

    val sessionDF = eventsDF
      .withColumn("firstTS", when(col("product") === lag("product", 1).over(userWindow), lit(null)).otherwise(unix_timestamp(col("eventTime"))))
      .orderBy(col("category"), col("userId"), col("eventTime"), col("firstTS").desc)
      .withColumn("sessionStart", last(col("firstTS"), ignoreNulls = true).over(userWindow.rowsBetween(Window.unboundedPreceding, 0)))
//      .withColumn("sessionStart", last())

//      .withColumn("sessionId", when(col("product") === col("prevProduct"), sessionIterator(lit(0))).otherwise(sessionIterator(lit(1))))
//      .drop("prevProduct", "isNewSession")
//    sessionDF.show()

    sessionDF
//      .groupBy("sessionId")
//      .agg((unix_timestamp(max(col("eventTime"))) - unix_timestamp(min(col("eventTime")))).as("sessionDuration"))


    //      .withColumn("sessionDurationSec", unix_timestamp(col("sessionEndTime")) - unix_timestamp(col("sessionStartTime")))
    //      .drop("sessionStartTime", "sessionEndTime")
    //      .orderBy("category", "userId", "eventTime")

    //    val sumDurationDF = sessionDF
//      .select("category", "product", "userId", "sessionId", "sessionDurationSec")
//      .distinct()
//
//    sumDurationDF
  }

}
