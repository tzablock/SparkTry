package udf

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

case class Test(str: String, num: Int, dat: Timestamp)

object Start {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("test").getOrCreate()
    import spark.implicits._
    val in = spark.read.option("inferSchema","true").csv("/home/tzablock/IdeaProjects/SparkTry/src/main/resources/some.csv")
      .withColumnRenamed("_c0","str")
      .withColumnRenamed("_c1","num")
      .withColumnRenamed("_c2","dat")
      .as[Test]

    in.show()

    val upp = udf((a: String) => a.toUpperCase)
    val inUpp = in.withColumn("UPPER",upp($"str"))
    inUpp.show()
  }
}
