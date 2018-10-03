package com.sparktest.datasets

import org.apache.spark.sql.{Dataset, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object SparkDataSetsPractice {
  
  case class Friends(name:String, friends:String)
  
    def createData(spark: SparkSession) = {
     
      import spark.implicits._
      
				val seqArray = Seq(("Yoda","Obi-Wan Kenobi"),
					("Anakin Skywalker", "Sheev Palpatine"),
					("Luke Skywalker",   "Han Solo, Leia Skywalker"),
					("Leia Skywalker",   "Obi-Wan Kenobi"),
					("Sheev Palpatine",  "Anakin Skywalker"),
					("Han Solo",         "Leia Skywalker, Luke Skywalker, Obi-Wan Kenobi, Chewbacca"),
					("Obi-Wan Kenobi",   "Yoda, Qui-Gon Jinn"),
					("R2-D2",            "C-3PO"),
					("C-3PO",            "R2-D2"),
					("Darth Maul",       "Sheev Palpatine"),
					("Chewbacca",        "Han Solo"),
					("Lando Calrissian", "Han Solo"),
					("Jabba",            "Boba Fett"))
						
		spark.createDataFrame(seqArray).as[Friends]
    
	}

	def sparkDatasetExperiments[T](spark:Dataset[T])={

	}


	def main(args:Array[String]): Unit={

			val spark = SparkSession
					.builder()
					.appName("Spark SQL basic example")
					.config("spark.some.config.option", "some-value")
					.getOrCreate()
			val dataset = createData(spark)
			sparkDatasetExperiments(dataset)

	}



}