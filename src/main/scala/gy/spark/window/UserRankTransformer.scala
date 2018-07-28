package gy.spark.window

import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.{DataFrame, SparkSession}

class UserRankTransformer(spark: SparkSession) {

  def transform(sessionDF: DataFrame): DataFrame = {

    import org.apache.spark.sql.functions._
    import spark.implicits._

    sessionDF
      .withColumn("sessionDurationSec", unix_timestamp($"sessionEndTime") - unix_timestamp($"sessionStartTime"))
      .select("category", "userId", "sessionId", "sessionDurationSec")
      .distinct()
      .groupBy("category", "userId")
      .agg((sum("sessionDurationSec") / 60).cast(DecimalType(32, 2)).as("userSpentMin"))
      .select("category", "userId", "userSpentMin")
      .createTempView("total_usage")

    val lessThan1DF = spark.sql("select category, count(userId) as users_less_than_1 from total_usage where userSpentMin < 1 group by category")
    val from1to5DF = spark.sql("select category, count(userId) as users_from_1_to_5 from total_usage where userSpentMin >= 1 AND  userSpentMin < 5 group by category")
    val moreThan5DF = spark.sql("select category, count(userId) as users_more_than_5 from total_usage where userSpentMin > 5 group by category")

    lessThan1DF.join(from1to5DF, Seq("category"), "outer").join(moreThan5DF, Seq("category"), "outer").na.fill(0)
  }

}
