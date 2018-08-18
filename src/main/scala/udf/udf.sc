
import java.sql.Date

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
case class Test(str: String, num: Int, dat: Date)

val spark = SparkSession.builder().master("local").appName("test").getOrCreate()
import spark.implicits._
val in = spark.read.csv("/home/tzablock/IdeaProjects/SparkTry/src/main/resources/some.csv").as[Test]

in.show()

val upp = udf((a: String) => a.toUpperCase)
val inUpp = in.withColumn("UPPER",upp($"str"))
inUpp.show()
