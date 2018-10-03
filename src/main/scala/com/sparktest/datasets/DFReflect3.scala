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

object DFReflect3 {
  
    val s2cc = new Schema2CaseClass
    import s2cc.implicits._
    
  trait initialdf{
      def key: Int
      def v1:Option[Seq[dffour]]
      def v2:Option[Seq[dffour]]
    }
    
  //case class finaldf2(v2:Option[Seq[dftwo]]) extends initialdf
  //case class finaldf3(key:Int,v1:Option[Seq[dfone]],v2:Option[Seq[dftwo]],v3:Option[Seq[dfthree]]) extends initialdf
  case class finaldf4(key:Int,v1:Option[Seq[dffour]],v2:Option[Seq[dffour]],v3:Option[Seq[dffour]]) extends initialdf


  case class dfone(key:Int,map:Map[String,String])
  case class dftwo(key:Int,map:Map[String,String])
  case class dfthree(key:Int,map:Map[String,String])
  case class dffour(key:Int, v1:Seq[dfone])
  case class dffive(key:Int, v2:Seq[dftwo])
  case class dfsix(key:Int, v3:Seq[dfthree])
  case class finaldf(key:Int, v1:Seq[dfone], v2:Seq[dftwo])
  //case class finaldf3(v3:Option[Seq[dfthree]]) with finaldf
    
 implicit val MapEncoder = org.apache.spark.sql.Encoders.kryo[Map[Int,String]]
 implicit val Map1Encoder = org.apache.spark.sql.Encoders.kryo[Map[String,Option[String]]]
 implicit def optionalInt: org.apache.spark.sql.Encoder[Option[Seq[dfthree]]] = org.apache.spark.sql.catalyst.encoders.ExpressionEncoder()
  
def test_groupBy(spark:SparkSession) = {
  import spark.implicits._  
  
  val fieldList = Seq("name","value","value1")
  val fieldList1 = Seq("name1","value23","value34")
  val fieldList2 = Seq("name2","value45","value56","name3")
  
  //val df1 = Seq((1,"ty",34,45),(1,"ty",34,45),(2,"rt",45,67),(2,"rt1",90,87)).toDF("key", "name","value","value1")
  val df1 = Seq((1,"ty","34","45"),(1,"ty","34","45"),(2,"rt","45","67"),(2,"rt1","90","")).toDF("key", "name","value","value1")
  val df2 = Seq((1,"ty","22","2"),(1,"ty","22","2"),(2,"rt","34","44")).toDF("key", "name1","value23","value34")
  val df3 = Seq((1,"ty","34","45","ty"),(2,"rt","45","67","yy"),(2,"rt","45","67","yy")).toDF("key", "name2","value45","value56","name3")

  val df1Map = df1.map(record => (record.getAs[Int](0),record.getValuesMap[String](fieldList)))
  val df2Map = df2.map(record => (record.getAs[Int](0),record.getValuesMap[String](fieldList1)))
  val df3Map = df3.map(record => (record.getAs[Int](0),record.getValuesMap[String](fieldList2)))

  
  val ds1: Dataset[dfone] = df1Map.map(rec => dfone(rec._1,rec._2))
  val ds2: Dataset[dfone] = df2Map.map(rec => dfone(rec._1,rec._2))
  val ds3: Dataset[dfone] = df3Map.map(rec => dfone(rec._1,rec._2))

  val df1grp = ds1.groupByKey(row => row.key)
  val df2grp = ds2.groupByKey(row => row.key)
  val df3grp = ds3.groupByKey(row => row.key)
  
  val df1keygrp = df1grp.mapGroups({case(k,iter) => dffour(k, iter.map(x => x).toSeq)})
  val df2keygrp = df2grp.mapGroups({case(k,iter) => dffour(k, iter.map(x => x).toSeq)})
  val df3keygrp = df3grp.mapGroups({case(k,iter) => dffour(k, iter.map(x => x).toSeq)})
  //df1keygrp.show()
  
  val joinedDf = df1keygrp.join(df2keygrp,"key").join(df3keygrp,"key")
  val joinedDs = joinedDf.as[finaldf4]
  //joinedDs.show()
   
  //val listofDS = Seq(df1keygrp,df2keygrp,df3keygrp)
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
    val im = m.reflect(x)
    val inquiry_dateMirror = im.reflectField(inquiry_date)
    //reflectField(inquiry_date)
    val returnval = inquiry_dateMirror.get
    returnval
  }
 
  
  def process[T](ds:Dataset[T],spark:SparkSession) = {
    import spark.implicits._ 
    val listOfObjects = Seq("v1","v2")
    
    val xx = ds.map{ case(x:finaldf4) => 
    val finaldfFields = for{field <- listOfObjects} yield getCaseClassField[finaldf4](x,field)
/*    val trade = x.v1
    val inq = x.v2*/
    val finalDfCaseInstance = finaldfFields(0)//for(x <- finaldfFields) yield x.asInstanceOf[Seq[dfone]]
    for(x <- finalDfCaseInstance.asInstanceOf[Seq[dffour]]){
      val f = getCaseClassField[dffour](x,"map")
      println(f)
    }   
/*    for(i <- trade.get){
      val ik = getCaseClassField[dfone](i,"map")
      println(ik)
    }
    val ik = 0
    ik*/
    val f = 0
    f
    }
    xx.show()  
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