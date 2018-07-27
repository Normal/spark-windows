package gy.spark.window

import java.util.UUID

import org.apache.spark.sql.{DataFrame, SparkSession}

class AddSessionTransformer(spark: SparkSession) {

  def transform(eventsDF: DataFrame): DataFrame = {

    import org.apache.spark.sql.functions._

    eventsDF.createOrReplaceTempView("events")

    val randUid = udf(() => UUID.randomUUID().toString.substring(0, 5))

    val sessionDF = eventsDF
      .groupBy(col("category"), col("userId"), window(col("eventTime"), "5 minutes"))
      .agg(randUid() as "sessionId")
      .select(
        col("category"),
        col("userId"),
        col("sessionId"),
        col("window.start") as "sessionStartTime",
        col("window.end") as "sessionEndTime"
      )
    sessionDF.createOrReplaceTempView("sessions")

    val fields = List(
      "events.category",
      "events.product",
      "events.userId",
      "events.eventTime",
      "events.eventType",
      "sessions.sessionId",
      "sessions.sessionStartTime",
      "sessions.sessionEndTime"
    ).mkString(",")

    val joinClauses = List(
      "events.userId == sessions.userId",
      "events.category == sessions.category",
      "events.eventTime < sessions.sessionEndTime",
      "events.eventTime >= sessions.sessionStartTime"
    ).mkString(" AND ")

    spark.sql(s"SELECT $fields FROM events LEFT JOIN sessions ON $joinClauses")
  }
}
