package structured.udf

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.udf

case class Test(str: String, num: Int, dat: Timestamp)
case class Test1(str: String, conv: String, dat: Timestamp)
/*
   creating udf
 */
object Start {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("test").getOrCreate()
    val somePath = getClass.getResource("/some.csv").toString
    val convPath = getClass.getResource("/conv.csv").toString

    import spark.implicits._

    val in = spark.read.option("inferSchema","true").csv(somePath)
      .withColumnRenamed("_c0","str")
      .withColumnRenamed("_c1","num")
      .withColumnRenamed("_c2","dat")
      .as[Test]   //TODO cast into object with timestamp
    in.show()
    //TODO register simple DSopperation.udf to upper case
    val upp = udf((a: String) => a.toUpperCase)
    val inUpp = in.withColumn("UPPER",upp($"str"))
    inUpp.show()

    //TODO make complicated DSopperation.udf with use of other DS
    val in1 = spark.read.option("inferSchema","true").csv(convPath)
      .withColumnRenamed("_c0","str")
      .withColumnRenamed("_c1","conv")
      .withColumnRenamed("_c2","dat")
      .as[Test1]

    val compUdf = udf(                 //TODO In spark we can't push SparkSession,DataFrame etc as closure into other function scope (it will be new object without internals)
      (a: String, b: Timestamp) => {
        in1.show()
        in1.filter(_.str == a).filter(_.dat == b).first().conv
      }
    )
    val convIn = in.withColumn("Convention",compUdf($"str",$"dat"))
    convIn.show()
  }
}
