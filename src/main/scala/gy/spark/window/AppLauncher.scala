package gy.spark.window

import com.typesafe.config.ConfigFactory
import grizzled.slf4j.Logger
import gy.spark.window.pipeline._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object AppLauncher {

  val logger: Logger = Logger[this.type]

  def main(args: Array[String]): Unit = {

    val config = ConfigFactory.load
    val showNumber = config.getInt("settings.show_items")

    val conf = new SparkConf()
      .setAppName("Spark window functions example")
    implicit val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val dataLoader = new DataLoader
    val sessionAdder = new AddSessionTransformer
    val medianDuration = new MedianDurationTransformer
    val userBucketing = new UserBucketingTransformer
    val productRanking = new ProductRankingTransformer(
      top = config.getInt("settings.top_products")
    )

    val eventDF = dataLoader.load(
      file = config.getString("data.file_path"),
      delimiter = config.getString("data.delimiter"),
      withHeader = config.getBoolean("data.header")
    ).persist()

    logger.info("Input data:")
    eventDF.show(showNumber)

    logger.info("Calculate sessions ...")
    val sessionDF = sessionAdder.transform(eventDF).persist()
    sessionDF.show(showNumber)

    logger.info("Calculate medians ...")
    val medianDF = medianDuration.transform(sessionDF)
    medianDF.show(showNumber)

    logger.info("Calculate users buckets ...")
    val userBucketDF = userBucketing.transform(sessionDF)
    userBucketDF.show(showNumber)

    logger.info("Calculate products rankings ...")
    val productRankDF = productRanking.transform(eventDF)
    productRankDF.show(showNumber)
  }
}
