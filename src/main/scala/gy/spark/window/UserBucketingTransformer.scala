package gy.spark.window

import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.{DataFrame, SparkSession}

class UserBucketingTransformer(spark: SparkSession) {

  def transform(sessionDF: DataFrame): DataFrame = {

    import org.apache.spark.sql.functions._
    import spark.implicits._

    val totalUsageDF = sessionDF
      .withColumn("sessionDurationSec", unix_timestamp($"sessionEndTime") - unix_timestamp($"sessionStartTime"))
      .select("category", "userId", "sessionId", "sessionDurationSec")
      .distinct()
      .groupBy("category", "userId")
      .agg((sum("sessionDurationSec") / 60).cast(DecimalType(32, 2)).as("userSpentMin"))
      .select("category", "userId", "userSpentMin")
  // DEBUG    totalUsageDF.show(30)

    val bucketing = udf((minutes: Double) => minutes match {
      case x if x < 1.0 => 1
      case x if x >= 1.0 && x <= 5.0 => 2
      case x if x > 5.0 => 3
    })

    totalUsageDF.createTempView("total_usage")
    spark.udf.register("bucketing", bucketing)
    val query =
      """
        |select category,
        |count(case when bucketing(userSpentMin) == 1 then userId end) as less_than_1,
        |count(case when bucketing(userSpentMin) == 2 then userId end) as from_1_to_5,
        |count(case when bucketing(userSpentMin) == 3 then userId end) as more_than_5
        |from total_usage
        |group by category
      """.stripMargin

    spark.sql(query)
  }

}
