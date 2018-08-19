package sparkgeneral

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Logging {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("dd").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  }

}
