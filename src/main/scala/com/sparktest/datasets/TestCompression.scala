package com.sparktest.datasets

import org.apache.spark.sql.{Dataset, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object TestCompression {
  
  def readFiles(spark:SparkSession,path:String)={
    
    val df = spark.read.textFile(path).toDF()
    df.write.parquet(path+"output")
    
    
  }
  
  
  	def main(args:Array[String]): Unit={

			val spark = SparkSession
					.builder()
					.appName("Spark SQL basic example").master("local").getOrCreate()
					
			readFiles(spark, args(0))
			//sparkDatasetExperiments(dataset)

	}
}