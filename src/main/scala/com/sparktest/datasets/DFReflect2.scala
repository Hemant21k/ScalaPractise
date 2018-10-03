//https://stackoverflow.com/questions/36648128/how-to-store-custom-objects-in-dataset
//https://stackoverflow.com/questions/36449368/how-to-create-encoder-for-option-type-constructor-e-g-optionint?rq=1
//https://stackoverflow.com/questions/28166555/how-to-convert-row-of-a-scala-dataframe-into-case-class-most-efficiently
//https://sumitpal.wordpress.com/2016/06/17/some-data-gymnastics-with-spark-2-0s-datasets-and-equivalent-code-in-rdds/
//https://stackoverflow.com/questions/38383207/rolling-your-own-reducebykey-in-spark-dataset

package com.sparktest.datasets

import scala.reflect.runtime.{universe => ru}
import org.apache.spark.sql.{Dataset, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object DFReflect2 {

  case class dfone(key:Int,name:String,value:Int,value1:Option[Int])
  case class dftwo(key:Int,name1:String,value23:Int,value34:Int)
  case class dfthree(key:Int,name2:String,value45:Int,value56:Int,name3:String)
  case class dffour(key:Int, v1:Seq[dfone])
  case class dffive(key:Int, v2:Seq[dftwo])
  case class dfsix(key:Int, v3:Seq[dfthree])
  case class finaldf(key:Int, v1:Seq[dfone], v2:Seq[dftwo], v3:Seq[dfthree])
  
   //val dfoneImplicitEncoder = Encoders.bean(dfone.getClass)
  
 implicit val MapEncoder = org.apache.spark.sql.Encoders.kryo[Map[Int,String]]
 implicit def optionalInt: org.apache.spark.sql.Encoder[Option[Int]] = org.apache.spark.sql.catalyst.encoders.ExpressionEncoder()



  
def test_groupBy(spark:SparkSession) = {
  import spark.implicits._  
  
  val df1 = Seq((1,"ty",34,45),(1,"ty",34,45),(2,"rt",45,67),(2,"rt1",90,87)).toDF("key", "name","value","value1")
  val df2 = Seq((1,"ty",22,2),(1,"ty",22,2),(2,"rt",43,44)).toDF("key", "name1","value23","value34")
  val df3 = Seq((1,"ty",34,45,"ty"),(2,"rt",45,67,"yy"),(2,"rt",45,67,"yy")).toDF("key", "name2","value45","value56","name3")

  val dsexpr1 = df1.as('person)
  val ds1: Dataset[dfone] = df1.as[dfone]
  val ds2: Dataset[dftwo] = df2.as[dftwo]
  val ds3: Dataset[dfthree] = df3.as[dfthree]

  val df1grp = ds1.groupByKey(row => row.key)
  val df2grp = ds2.groupByKey(row => row.key)
  val df3grp = ds3.groupByKey(row => row.key)
  
  val keyValues = List( (3, "Me"),(1, "Thi"),(2, "Se"),(3, "ssa"),(1, "sIsA"),(3, "ge:"),(3, "-)"),(2, "cre"),(2, "t") )
  val keyValuesDS = keyValues.toDS
  val keuvaluesGroupedSet = keyValuesDS.groupByKey(_._1).mapValues(_._2) //.reduceGroups((acc,str) => acc+str)

  val df1keygrp_copy = df1grp.mapGroups({case(k,iter) => dffour(k, iter.map(x => dfone(x.key,x.name,x.value,x.value1)).toSeq)})

  val df1keygrp = df1grp.mapGroups({case(k,iter) => dffour(k, iter.map(x => dfone(x.key,x.name,x.value,x.value1)).toSeq)})
  val df2keygrp = df2grp.mapGroups({case(k,iter) => dffive(k, iter.map(x => dftwo(x.key,x.name1,x.value23,x.value34)).toSeq)})
  val df3keygrp = df3grp.mapGroups({case(k,iter) => dfsix(k, iter.map(x => x).toSeq)})
  
  
  val joinedDf = df1keygrp.join(df2keygrp,"key").join(df3keygrp,"key")
  val joinedDs = joinedDf.as[finaldf]
  
  val listofDS = Seq(df1keygrp,df2keygrp,df3keygrp)
  
  //joinDatasets(spark)(listofDS:_*)
  process(joinedDs,spark)
    
  }
  
  def joinDatasets[T](spark:SparkSession)(ds:Dataset[T]*)={
    import spark.implicits._ 
    val numOfDS = ds.length
    val joinedDS = numOfDS match {
      case 2 => ds(0).join(ds(1),"key")
      case 3 => ds(0).join(ds(1),"key").join(ds(2),"key").as[finaldf]
      case _ => spark.emptyDataset[String]
    }
    joinedDS.show()
  }
  
   def getCaseClassField[T](x:Object,field:String)(implicit tt: ru.TypeTag[T]) = {
    val m = ru.runtimeMirror(x.getClass.getClassLoader)
    val inquiry_date = ru.typeOf[T].declaration(ru.newTermName(field)).asTerm
    //decl(ru.newTermName(field)).asTerm
    val im = m.reflect(x)
    val inquiry_dateMirror = im.reflectField(inquiry_date)
    //reflectField(inquiry_date)
    val returnval = inquiry_dateMirror.get
    returnval
  }
 
  
  def process[T](ds:Dataset[T],spark:SparkSession) ={
    import spark.implicits._ 
    val xx = ds.map{ case(x:finaldf) => 
    val trade = x.v1
    val inq = x.v2
    val pub = x.v3
    //val ik:T = None
    for(i <- trade){
      val ik = getCaseClassField[dfone](i,"value1")
      println(ik)
    }
    val ik = 0
    ik
    }
    //}
    
    println(xx.show())
    
  }


	def main(args:Array[String]): Unit={

			val spark = SparkSession
					.builder()
					.appName("Spark SQL basic example").master("local")
					.getOrCreate()
					
			test_groupBy(spark)
			//sparkDatasetExperiments(dataset)

	}

}