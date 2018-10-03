package some.tests

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{ col, when }
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType

class ExamplesSpark(spark: SparkSession) {

  def transform() = {
    val DS = spark.range(1, 1000, 2).toDF("key")
    val DF = DS.withColumn("key1", when(col("key") % 2 === 0, col("key") / 2).otherwise(col("key") % 3))
    DF.select(col("*"), getColumn(DF)).show(12)
    filterRecords(DF)
  }

  def getColumn(DF: DataFrame): Column = {
    when(DF("key1") === 0, DF("key1") + 2).otherwise(DF("key") + 3).as('key3)
  }

  def filterRecords(DF: DataFrame): DataFrame = {
    DF.filter(col("key").cast(IntegerType) > 23).select("*").show()
    val newDF = DF.select(col("key").cast(StringType),col("key1").cast("long"))
    newDF.printSchema
    DF.select("*").where(col("key") === 3 || col("key") === 13).show()
    DF.select("*").where(col("key") === 3 || col("key") === 13)
  }
}

object ExampleSpark {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.master("local[*]").appName("test job").getOrCreate()
    val exampleSpark = new ExamplesSpark(spark).transform()
  }
}