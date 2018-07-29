package gy.spark.window.pipeline

import java.util.UUID

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}

class AddSessionTransformer()(implicit spark: SparkSession) {

  def transform(eventDF: DataFrame): DataFrame = {

    import org.apache.spark.sql.functions._
    import spark.implicits._

    val windowSpec = Window.partitionBy("category", "userId").orderBy("eventTime")
    val randUid = udf(() => UUID.randomUUID().toString.substring(0, 13))

    /*
        Calculates session grouping by userId and category.
        If distance between 2 particular events inside any group is more than 5 minutes (300 sec)
        that means new session was started.

        |category|product|userId|eventTime|eventType|sessionId|
     */
    val sessionIdDF = eventDF
      .withColumn(
        "newSessionId",
        when(
          unix_timestamp($"eventTime") - unix_timestamp(lag("eventTime", 1)
            .over(windowSpec)) < 300, lit(null)
        ).otherwise(randUid())
      )
      .withColumn(
        "sessionId",
        last($"newSessionId", ignoreNulls = true)
          .over(windowSpec.rowsBetween(Window.unboundedPreceding, 0))
      )
      .drop("newSessionId")

    /*
        Assumed that sessions with only one event has same start-end time and 0 duration.

        Spark df api variant:
          val sessionWindow = Window.partitionBy("sessionId")
          sessionIdDF
            .withColumn("sessionStartTime", min("eventTime").over(sessionWindow))
            .withColumn("sessionEndTime", max("eventTime").over(sessionWindow))
            .orderBy($"sessionStartTime", $"sessionId", $"eventTime")


        |category|product|userId|eventTime|eventType|sessionId|sessionStartTime|sessionEndTime|
    */
    sessionIdDF.createTempView("sessions")
    val sql =
      """
        |SELECT
        | *,
        | min(eventTime) OVER (PARTITION BY sessionId) AS sessionStartTime,
        | max(eventTime) OVER (PARTITION BY sessionId) AS sessionEndTime
        |FROM sessions
        |ORDER BY sessionStartTime, sessionId, eventTime
      """.stripMargin
    spark.sql(sql)
  }
}
