package structured

import org.apache.spark.sql.SparkSession

class ReadingStructuredData {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("ss").master("local").getOrCreate()
    //User friendly:
    spark.read.csv("path")
    spark.read.json("path")
//    spark.read.jdbc()//TODO check

    //hadoop optimized parquet,orc,avro
    spark.read.parquet("") //TODO diff to other
    spark.read.orc()
  }

}
