package gy.spark.window

import java.util.UUID

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.unix_timestamp
import org.apache.spark.sql.{DataFrame, SparkSession}

class AddSessionTransformer(spark: SparkSession) {

  def transform(eventsDF: DataFrame): DataFrame = {

    import org.apache.spark.sql.functions._
    import spark.implicits._

    //    eventsDF.createOrReplaceTempView("events")

    val windowSpec = Window.partitionBy("category", "userId").orderBy("eventTime")

    val randUid = udf(() => UUID.randomUUID().toString.substring(0, 5))

    var sessionId = 1

    val windowSpec2 = Window.partitionBy("sessionId")

    eventsDF
      .select(
        col("category"),
        col("product"),
        col("userId"),
        col("eventTime"),
        col("eventType"),
        lag("eventTime", 1).over(windowSpec).as("prev_eventTime")
      )
      .withColumn("isNewSession", when(unix_timestamp($"eventTime") - unix_timestamp($"prev_eventTime") < 300, lit(0)).otherwise(lit(1)))
      .withColumn("genSessionId", randUid())
      .withColumn("sessionId", when(col("isNewSession") === lit(1), col("genSessionId")).otherwise(first("genSessionId").over(windowSpec)))
      .withColumn("sessionStartTime", min("eventTime").over(windowSpec2))
      .withColumn("sessionEndTime", max("eventTime").over(windowSpec2))
      .drop("isNewSession", "genSessionId", "prev_eventTime")

//      .select(
//        col("category"),
//        col("product"),
//        col("userId"),
//        col("eventTime"),
//        col("eventType"),
//        unix_timestamp(col("eventTime")) - unix_timestamp(col("prev_eventTime"))
//
////          when(unix_timestamp(col("eventType")) - unix_timestamp(col("prev_eventTime")) < 300, lit(0)).otherwise(lit(1)).as("isNewSession")
//      )

    // TODO: order events by category and events_time asc - iterate using lag
    // TODO: if session - prev_session > 5 or prev_brand != brand then new session
    // http://www.janvsmachine.net/2017/09/sessionization-with-spark.html

    //
    //    val sessionDF = eventsDF
    //      .groupBy(col("category"), col("userId"), window(col("eventTime"), "5 minutes"))
    //      .agg(randUid() as "sessionId")
    //      .select(
    //        col("category"),
    //        col("userId"),
    //        col("sessionId"),
    //        col("window.start") as "sessionStartTime",
    //        col("window.end") as "sessionEndTime"
    //      )
    //    sessionDF

    //      .createOrReplaceTempView("sessions")
    //
    //    val fields = List(
    //      "events.category",
    //      "events.product",
    //      "events.userId",
    //      "events.eventTime",
    //      "events.eventType",
    //      "sessions.sessionId",
    //      "sessions.sessionStartTime",
    //      "sessions.sessionEndTime"
    //    ).mkString(",")
    //
    //    val joinClauses = List(
    //      "events.userId == sessions.userId",
    //      "events.category == sessions.category",
    //      "events.eventTime < sessions.sessionEndTime",
    //      "events.eventTime >= sessions.sessionStartTime"
    //    ).mkString(" AND ")
    //
    //    spark.sql(s"SELECT $fields FROM events LEFT JOIN sessions ON $joinClauses")
  }
}
