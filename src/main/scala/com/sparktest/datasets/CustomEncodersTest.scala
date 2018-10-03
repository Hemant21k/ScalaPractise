package com.sparktest.datasets

import org.apache.spark.sql.{ Encoder, Encoders }
import scala.reflect.ClassTag
import org.apache.spark.sql.{ Dataset, DataFrame, SparkSession }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.encoders._

object CustomEncodersTest {

	class MyObj(val i: Int, val u: java.util.UUID, val s: Seq[String])
	
	class customObj(val i:Int)
	
	type MyObjEncoded = (Int, String, Seq[String])

	implicit def toEncoded(o: MyObj): MyObjEncoded = (o.i, o.u.toString, o.s)
	implicit def fromEncoded(e: MyObjEncoded): MyObj =
	new MyObj(e._1, java.util.UUID.fromString(e._2), e._3)
	
	implicit val myObjEncoder = org.apache.spark.sql.Encoders.kryo[MyObj]
	
	implicit def kryoEncoder[A](implicit ct: ClassTag[A]) = org.apache.spark.sql.Encoders.kryo[A](ct)

  def createDatasetsfromKryoEncoder(spark: SparkSession) = {
    val d1 = spark.createDataset(Seq(new customObj(1), new customObj(2), new customObj(3)))
    val d2 = d1.map(d => (d.i + 1, d)).alias("d2") // mapping works fine and ..
    val d3 = d1.map(d => (d.i, d)).alias("d3") // .. deals with the new type
    
    println(d2.printSchema)
    println(d3.printSchema)

    
    //val joinedDF = d2.join(d3,d2.col("i")=== d3.col("i"))

    //joinedDF.printSchema()

  }


	def EncodersImplicits(spark:SparkSession) = {
	  
	  import spark.implicits._
	  
	  val d = spark.createDataset(Seq[MyObjEncoded](
  new MyObj(1, java.util.UUID.randomUUID, Seq("foo")),
  new MyObj(2, java.util.UUID.randomUUID, Seq("bar"))
)).toDF("i","u","s").as[MyObjEncoded]
	  val d1 = spark.createDataset(Seq[MyObjEncoded](
  new MyObj(1, java.util.UUID.randomUUID, Seq("foo1")),
  new MyObj(2, java.util.UUID.randomUUID, Seq("bar1"))
)).toDF("i","u","s").as[MyObjEncoded]
	  
	  val joinedDF = d.join(d1, Seq("i"))
	  
	  joinedDF.printSchema()
	  joinedDF.show()

	}

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local")
      .getOrCreate()

    EncodersImplicits(spark)

    

    //createDatasetsfromKryoEncoder(spark)

  }

}