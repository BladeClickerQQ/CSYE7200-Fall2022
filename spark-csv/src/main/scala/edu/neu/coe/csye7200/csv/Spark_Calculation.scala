
import org.apache.spark.sql.SparkSession

import scala.util.{Success, Try}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.functions.stddev

object Spark_Calculation extends App {
  def apply() = ???


  val spark: SparkSession = SparkSession
    .builder()
    .appName("WordCount")
    .master("local[*]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  def SparkRead(path:String):sql.DataFrame =
    spark.read.option("header","true").csv(path)

  def DFMean(df:sql.DataFrame, column:String):Double =
    df.select(avg("imdb_score")).first().getDouble(0)

  def DFStd(df: sql.DataFrame, column: String): Double =
    df.select(stddev("imdb_score")).first().getDouble(0)

//  def SparkDfSingleColMean(columnName:String) =
//    df.select(avg("imdb_score"))

//  val df = spark.read.option("header","true").csv("/Users/laibinghui/Desktop/CSYE7200-Fall2022/spark-csv/src/main/resources/movie_metadata.csv")

  val path = "/Users/laibinghui/Desktop/CSYE7200-Fall2022/spark-csv/src/main/resources/movie_metadata.csv"
  val df = SparkRead(path)
  val mean = DFMean(df,"imdb_score")
  val std = DFStd(df,"imdb_score")

//  val mean = df.select(avg("imdb_score"))
//  val std = df.select(stddev("imdb_score"))

  df.show()
  println(mean)
  println(std)
}