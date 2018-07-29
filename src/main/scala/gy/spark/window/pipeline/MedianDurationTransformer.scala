package gy.spark.window.pipeline

import org.apache.spark.sql.{DataFrame, SparkSession}

class MedianDurationTransformer(implicit spark: SparkSession) {

  def transform(sessionDF: DataFrame): DataFrame = {

    import org.apache.spark.sql.functions._
    import spark.implicits._

    /*
        |category|sessionId|sessionDurationSec|
     */
    val durationDF = sessionDF
      .withColumn("sessionDurationSec", unix_timestamp($"sessionEndTime") - unix_timestamp($"sessionStartTime"))
      .select("category", "sessionId", "sessionDurationSec")
      .distinct()
    durationDF.createTempView("durations")

    /*
        Calculates median duration as AVG. Remove fractional part of result.

        Spark df api variant:
          .groupBy("category")
          .agg(avg("sessionDurationSec").cast(DecimalType(32, 0)).as("medianDurationSec"))
          .select("category", "medianDurationSec")

        |category|medianDurationSec|
     */
    val sql =
      """
        |SELECT category, CAST(avg(sessionDurationSec) AS DECIMAL(32, 0)) AS medianDurationSec
        |FROM durations
        |GROUP BY category
      """.stripMargin
    spark.sql(sql)
  }
}
