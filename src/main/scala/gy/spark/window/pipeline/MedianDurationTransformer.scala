package gy.spark.window.pipeline

import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.{DataFrame, SparkSession}

class MedianDurationTransformer(implicit spark: SparkSession) {

  def transform(sessionDF: DataFrame): DataFrame = {

    import org.apache.spark.sql.functions._
    import spark.implicits._

    sessionDF
      .withColumn("sessionDurationSec", unix_timestamp($"sessionEndTime") - unix_timestamp($"sessionStartTime"))
      .select("category", "sessionId", "sessionDurationSec")
      .distinct()
      .groupBy("category")
      .agg(avg("sessionDurationSec").cast(DecimalType(32, 0)).as("medianDurationSec"))
      .select("category", "medianDurationSec")
  }
}
