package gy.spark.window

import org.apache.spark.sql.{DataFrame, SparkSession}

class AddSessionTransformer(spark: SparkSession) {

  def transform(df: DataFrame): DataFrame = {

    import org.apache.spark.sql.expressions.Window
    import org.apache.spark.sql.functions._

//    val windowSpec = Window.partitionBy(col("category")).orderBy(col("eventTime").asc)
////    val sessionDuration = "5 minutes"
//
//    df.select(
//      col("userId"),
//      col("eventTime"),
//      col("category"),
//      first("eventTime").over(windowSpec).as("firstEventTime"),
//      lag("eventTime", 1).over(windowSpec).as("prevEventTime")
//    )

    df
      .groupBy(col("category"), window(col("eventTime"), "5 minutes"))
      .pivot("userId")
      .agg(rand() as "session_id")
      .select("window.start", "window.end", "category", "session_id", "userId")
  }
}
