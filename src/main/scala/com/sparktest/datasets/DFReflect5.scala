//https://stackoverflow.com/questions/36648128/how-to-store-custom-objects-in-dataset
//https://stackoverflow.com/questions/36449368/how-to-create-encoder-for-option-type-constructor-e-g-optionint?rq=1
//https://stackoverflow.com/questions/28166555/how-to-convert-row-of-a-scala-dataframe-into-case-class-most-efficiently
//https://sumitpal.wordpress.com/2016/06/17/some-data-gymnastics-with-spark-2-0s-datasets-and-equivalent-code-in-rdds/
//https://stackoverflow.com/questions/38383207/rolling-your-own-reducebykey-in-spark-dataset
//https://github.com/vaquarkhan/vk-wiki-notes/wiki/Apache-Spark-custom-Encoder-example

package com.sparktest.datasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions._

    import org.apache.spark.sql.types._
    import org.apache.spark.sql.{ Encoder, Encoders }
    import org.apache.spark.sql.catalyst.encoders.RowEncoder

 object Test1 {
  def main(args: Array[String]) {
  val spark =
   SparkSession.builder()
    .appName("SQL-JSON")
    .master("local[4]")
    .getOrCreate()

import spark.implicits._

// easy enough to query test1 JSON
val people = spark.read.json("src/main/resources/data/test1.json")
//print schema
 people.printSchema()
 println("------------------------------------------------------")
 //create table 
 people.createOrReplaceTempView("people")
 //fire query 
 val peoplesql = spark.sql("SELECT * FROM people")
 //print Json
  peoplesql.foreach(r => println(r))
  //print dataframe
  println("------------------------------------------------------")
  peoplesql.show();
  println("------------------------------------------------------")

 /*
  * DataFrame is to group by age, order by id and filter all age group with more than 1 student. I use the following 
  script:
   */
    val df = spark.sqlContext.read.json("src/main/resources/data/test1.json")

    val arrLen = udf { a: Seq[Row] => a.length > 1 }

    val mergedDF = df.withColumn("newCol", collect_set(struct("age", "id", 
    "name")).over(Window.partitionBy("age").orderBy("id"))).select("newCol", "age")

    val filterd = mergedDF.filter(arrLen(col("newCol")))

    filterd.show();
    println("------------------------------------------------------")

   /**
     * [WrappedArray([28,1203,amith], [28,1202,krishna]),28]
     * [WrappedArray([25,1201,satish], [25,1205,mendy]),25]
     * [WrappedArray([23,1204,javed], [23,1207,prudvi]),23]
     */
   /**
    *  merge those two students rows inside the WrappedArray into one,
    * taking for example the id of the first student and the name of the second student.
    */
    filterd.foreach { x =>
      val student = PrintOne(x.getAs[Seq[Row]](0), x.getAs[String]("age"))
     println("merged student: " + student)
    }

   //same into map throwing error without encoder
    //TODO val merged = filterd.map { row => (row.getAs[String]("age"), PrintOne(row.getAs[Seq[Row]](0), row.getAs[String]("age"))) }
   println("------------------------------------------------------")

    import org.apache.spark.sql.types._
    import org.apache.spark.sql.{ Encoder, Encoders }
    import org.apache.spark.sql.catalyst.encoders.RowEncoder

  val encoder = Encoders.tuple(
     Encoders.STRING,
      RowEncoder(
       // The same as df.schema in your case
        StructType(Seq(
           StructField("age", StringType),
           StructField("id", StringType),
           StructField("name", StringType)))))

    filterd.map { row =>
       (
        row.getAs[String]("age"),
         PrintOne(row.getAs[Seq[Row]](0), row.getAs[String]("age")))
      }(encoder)
  }

   println("------------------------------------------------------")

   /*
   * Function 
   */
   def PrintOne(List: Seq[Row], age: String): Row = {
    val studentsDetails = Array(age, List(0).getAs[String]("id"), List(1).getAs[String]("name"))
    val mergedStudent = new GenericRowWithSchema(studentsDetails.toArray, List(0).schema)

       mergedStudent
    }

 }