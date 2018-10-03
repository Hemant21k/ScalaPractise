//https://stackoverflow.com/questions/36648128/how-to-store-custom-objects-in-dataset
//https://stackoverflow.com/questions/36449368/how-to-create-encoder-for-option-type-constructor-e-g-optionint?rq=1
//https://stackoverflow.com/questions/28166555/how-to-convert-row-of-a-scala-dataframe-into-case-class-most-efficiently
//https://sumitpal.wordpress.com/2016/06/17/some-data-gymnastics-with-spark-2-0s-datasets-and-equivalent-code-in-rdds/
//https://stackoverflow.com/questions/38383207/rolling-your-own-reducebykey-in-spark-dataset

package com.sparktest.datasets

import scala.reflect.runtime.universe.TypeTag
import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => ru}
import org.apache.spark.sql.{Dataset, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ Encoder, Encoders }
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema


object DFReflect4 {
  
  	implicit class RowUtils(row:Row){
	  def getAsType[T](fieldname:String)={
	    if(!row.isNullAt(row.fieldIndex(fieldname))){
	  //if(row.get(row.fieldIndex(fieldname)) != null){ 
	  row.getAs[T](row.fieldIndex(fieldname))}
	  else
	    null.asInstanceOf[T]
	  }
	}

    case class Data1(`key.[1]`: String, value1: Int)
    case class Data2(key: String, value2: Int)
    case class groupeddf(key:String, v1:Seq[Map[String,Int]])
  
   //val dfoneImplicitEncoder = Encoders.bean(dfone.getClass)
  
 //implicit val RowEncoder = org.apache.spark.sql.Encoders.kryo[Row]
 implicit val MapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String,String]]
 implicit val Map1Encoder = org.apache.spark.sql.Encoders.kryo[Seq[Map[String,String]]]
 implicit def optionalInt: org.apache.spark.sql.Encoder[GenericRowWithSchema] = org.apache.spark.sql.catalyst.encoders.ExpressionEncoder()

def createQuery(tableName:String,fieldList: Seq[String],spark:SparkSession): DataFrame = {
      val fields = fieldList.mkString(",")
      spark.sql(s"select $fields from $tableName")
    }

def groupDataFrames(key:String,df:DataFrame,spark:SparkSession):KeyValueGroupedDataset[String, Row] = {
  import spark.implicits._
  df.groupByKey(row => row.getAs[String](key))
}

def mapGroupDataFrames[T <:Product: TypeTag : Encoder](df: KeyValueGroupedDataset[String, Row], fieldList: Seq[String], spark: SparkSession): Dataset[T] = {
    import spark.implicits._    
    val df1 = df.mapGroups({ case (k, iter) => (k, iter.map(x => x.getValuesMap[Int](fieldList)).toSeq)})
    //df1.toDF("key","v1")
    val fieldList1 = Seq("key","v1")
    val df2 = df1.toDF(fieldList1: _*)
    df2.printSchema()
   df2.as[T]
  }

/*def mapGroupDataFrames(df:KeyValueGroupedDataset[String, Row],fieldList:Seq[String],spark:SparkSession):Dataset[groupeddf] ={
  import spark.implicits._
  df.mapGroups({case(k,iter) => groupeddf(k,iter.map(x => x.getValuesMap[Int](fieldList)).toSeq)})
}*/

def queryBuilder(df:DataFrame,listOfFields:Seq[String])={
  val selectClause = listOfFields.map(column => s"`${column}`")
  //df.select("`key.[1]`","`value`")
/* val colNames = listOfFields.map(name => col(name))
  val col1: Seq[Column] = listOfFields.map(df(_))*/
  println(selectClause)
  df.select(selectClause.head,selectClause.tail:_*)
}


def test_groupBy(spark:SparkSession) = {
  import spark.implicits._  
  
  val fieldList = Seq("value1")
  val fieldList1 = Seq("value1")
  val fieldList2 = Seq(fieldList,fieldList1)

    val data1s = Range(0, 1000000).map(x => Data1(s"key${x}", x))
    val data2s = Range(0, 1000000).map(x => Data1(s"key${x}", x))

    val df1 = spark.sparkContext.parallelize(data1s).toDF
    val df2 = spark.sparkContext.parallelize(data2s).toDF
    
    df1.createOrReplaceTempView("df1")
    df2.createOrReplaceTempView("df2")
    
    val tableFieldMap = Map("df1" -> Seq("key","value1"),"df2"-> Seq("key","value1"))
    
   val dfList = for{(k,v) <- tableFieldMap} yield createQuery(k,v,spark)

   val groupedDataFrames = for{ df <- dfList} yield groupDataFrames("key",df,spark)
/*    val groupeddata1s = df1.groupByKey(row => row.getAs[String]("key"))
    val groupeddata2s = df2.groupByKey(row => row.getAs[String]("key2"))*/
   //val encoder = Encoders.tuple(Encoders.STRING,Encoders.kryo[Seq[Map[String,Int]]])
/*  val df1keygrp = groupeddata1s.mapGroups({case(k,iter) => groupeddf(k,iter.map(x => x.getValuesMap[Int](fieldList)).toSeq)})
  val df2keygrp = groupeddata2s.mapGroups({case(k,iter) => groupeddf(k, iter.map(x => x.getValuesMap[Int](fieldList1)).toSeq)})*/
   val zippedgroupedDataFrames = groupedDataFrames.zip(fieldList2)
   val mapGroupedDataFrames = for(f <- zippedgroupedDataFrames) yield mapGroupDataFrames[groupeddf](f._1,f._2,spark)
     
  //val joineddf  = mapGroupedDataFrames.toList(0).join(mapGroupedDataFrames.toList(1),Seq("key"))
  
  joinDatasets(spark)(mapGroupedDataFrames.toSeq:_*)
    
  }
  
  def joinDatasets[T](spark:SparkSession)(ds:Dataset[T]*)={
    import spark.implicits._ 
    val numOfDS = ds.length
    val joinedDS = numOfDS match {
      case 2 => ds(0).join(ds(1),"key")
      case 3 => ds(0).join(ds(1),"key").join(ds(2),"key")
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
 

	def main(args:Array[String]): Unit={

			val spark = SparkSession
					.builder()
					.appName("Spark SQL basic example").master("local")
					.getOrCreate()
		
					import spark.implicits._  
					
		val data1s = Range(0, 1000000).map(x => (s"key${x}", x,null.asInstanceOf[Integer]))

    val df1 = spark.sparkContext.parallelize(data1s).toDF("key.[1]","value","value.[1]")
    
    val retdf = queryBuilder(df1,Seq("key.[1]","value.[1]","value"))
    //retdf.write.parquet("D:\\dataoutput\\")
    
   // val dfread = spark.read.parquet("D:\\dataoutput\\")
    
    val struct = StructType(
     StructField("name", StringType, true) ::
     StructField("country", StringType, false) ::
     StructField("zip_code", LongType, false) :: Nil)
    
    val dfread = spark.read.option("header","true").schema(struct).csv("D:\\data\\data.txt")
    //dfread.show()
    //dfread.printSchema()
    //val x = dfread.select("`key.[1]`","value.[1]").where( dfread.col("value").leq(3))
   // x.show()
    val mp = dfread.map(y => y.getAs[Long]("zip_code"))
    //mp.toDF("zip_code").show
    
    val schema = StructType(List(StructField("name.wer.[9]", StringType, true)))
    val encoder = RowEncoder(schema) //https://jaceklaskowski.gitbooks.io/mastering-spark-sql/spark-sql-RowEncoder.html
    
    val x = dfread.mapPartitions{ partition =>
      {
      partition.map { row =>
        Row.fromSeq(Seq("xx"))
      }
			}
			}(encoder)
			
			x.show()
    
    /**https://stackoverflow.com/questions/39255973/split-1-column-into-3-columns-in-spark-scala?rq=1*/
    val newdfread = dfread.withColumn("temp", split(col("name"), "\\.")).select(
    Seq("name1","name2","name3").zipWithIndex.map{i => val name = i._1;
      col("temp").getItem(i._2).as(s"$name")}: _*)
      
      //newdfread.printSchema()
			//test_groupBy(spark)
			//sparkDatasetExperiments(dataset)

	}


	
	
}