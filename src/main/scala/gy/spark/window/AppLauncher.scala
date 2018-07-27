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

    val df = dataLoader.load(
      file = config.getString("data.file_path"),
      delimiter = config.getString("data.delimiter"),
      withHeader = config.getBoolean("data.header")
    )
    df.show()

    sessionAdder.transform(df).show()
  }

}
