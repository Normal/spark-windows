package gy.spark.window

import com.typesafe.config.ConfigFactory
import grizzled.slf4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object AppLauncher {

  val logger: Logger = Logger[this.type]

  def main(args: Array[String]): Unit = {

    val config = ConfigFactory.load

    val conf = new SparkConf().setAppName("Spark window functions example").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val dataLoader = new DataLoader(spark)
    val sessionAdder = new AddSessionTransformer(spark)
    val medianDuration = new MedianDurationTransformer(spark)
    val userBucketing = new UserBucketingTransformer(spark)

    val df = dataLoader.load(
      file = config.getString("data.file_path"),
      delimiter = config.getString("data.delimiter"),
      withHeader = config.getBoolean("data.header")
    )
    logger.info("Input data: ")
    df.show(30)

    logger.info("Calculate sessions ...")
    val sessionDF = sessionAdder.transform(df).persist()
    logger.info("Session data: ")
    sessionDF.show(30)

    logger.info("Calculate medians ...")
    val medianDF = medianDuration.transform(sessionDF)
    logger.info("Median data: ")
    medianDF.show(30)

    logger.info("Calculate users buckets ...")
    val userBucketDF = userBucketing.transform(sessionDF)
    logger.info("User bucket data: ")
    userBucketDF.show(30)
  }

}
