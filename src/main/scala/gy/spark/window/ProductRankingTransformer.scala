package gy.spark.window

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}

class ProductRankingTransformer(spark: SparkSession) {

  def transform(eventsDF: DataFrame): DataFrame = {

    import org.apache.spark.sql.functions._

    val userWindow = Window.partitionBy("userId").orderBy("eventTime")
    val sessionWindow = Window.partitionBy("userId").orderBy("sessionStart")

    /*
        Populates initial dataframe with session data.
        Session lasts until the user is looking at particular product.
        When particular user switches to another product the new session starts.

        |category|product|userId|eventTime|eventType|sessionStart|sessionEnd|
     */
    val sessionDF = eventsDF
      .withColumn("firstTS", when(col("product") === lag("product", 1).over(userWindow), lit(null)).otherwise(unix_timestamp(col("eventTime"))))
      .orderBy(col("category"), col("userId"), col("eventTime"), col("firstTS").desc)
      .withColumn("sessionStart", last(col("firstTS"), ignoreNulls = true).over(userWindow.rowsBetween(Window.unboundedPreceding, 0)))
      .withColumn("sessionEnd", unix_timestamp(max(col("eventTime")).over(sessionWindow)))
      .drop("firstTS")

    /*
        Grouped by session, calculated duration for each session.

        |category|product|userId|sessionDurationSec|
      */
    val durationDF = sessionDF
      .withColumn("sessionDurationSec", col("sessionEnd") - col("sessionStart"))
      .select("category", "product", "userId", "sessionDurationSec")
      .distinct()
      .orderBy("category", "userId", "eventTime")

    /*
        |category|product|totalTimeSpent|
     */
    val totalDurationDF = durationDF
      .groupBy("category", "product")
      .agg(sum(col("sessionDurationSec")).as("totalTimeSpent"))
      .orderBy(col("category"), col("totalTimeSpent").desc)

    /*
        |category|product|totalTimeSpent|rank|
     */
    val categoryWindow = Window.partitionBy("category").orderBy(col("totalTimeSpent").desc)
    val rankedDF = totalDurationDF
      .withColumn("rank", rank.over(categoryWindow))

    rankedDF
        .filter(col("rank") <= 2)
        .select("category", "product")
        .orderBy("category", "rank")

  }

}
