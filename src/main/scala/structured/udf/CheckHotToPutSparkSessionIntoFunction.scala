package structured.udf

import java.sql.Timestamp

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.udf

case class Te(_c0: String, _c1: Int, _c2: Timestamp)
/*
   To normal functions we can put DataSet and SparkSession as closure and it will be the same object
   When we put them to udf as closure it's empty new object !!!
 */
object CheckHotToPutSparkSessionIntoFunction {
  def main(args: Array[String]): Unit = {
    //TODO check how to put into function Context SparkSession (if it's possible to do it as param)
    val spark = SparkSession.builder().appName("test").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    println(spark)
    val in = spark.read.option("inferSchema","true").csv(getClass.getResource("/some.csv").toString)
    import spark.implicits._
    val rin = in.as[Te]
    println(in)
    def fun={
      println(spark)
      println(in)
      in.foreach(t => println(t))
    }
    fun


    //TODO solution for udf  (it's only problem with sparkSession and DataFrames...)
    val l2 = List(4,5,6)
    def someUds(l: List[Int]) = udf((s: String) => {
      println(s)
      l.foreach(println)
      l2.foreach(println)
      "sssss"
    }
    )
    in.withColumn("new",someUds(List(1,2,3))($"_c0")).show()
  }

}
