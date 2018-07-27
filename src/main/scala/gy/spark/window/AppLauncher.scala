package gy.spark.window

import grizzled.slf4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object AppLauncher {

  val logger: Logger = Logger[this.type]

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("ALS data filter")

    val spark = SparkSession.builder().config(conf).getOrCreate()
    logger.info(s"Spark configuration: ${spark.conf.getAll}")

    val result = spark.sparkContext.parallelize(List(1, 2, 3, 4, 5))
    logger.info(s"output: ${result.count()}")
  }

}
