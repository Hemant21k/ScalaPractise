package some.tests

import org.apache.spark.sql.{Dataset, DataFrame, SparkSession}
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.util.ExecutionListenerManager
import org.apache.spark.sql.util.QueryExecutionListener
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.execution.{QueryExecution, WholeStageCodegenExec}
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import java.util.Date
import java.text.SimpleDateFormat
import java.sql.Date
import org.apache.spark.rdd.RDD
import org.apache.commons.codec.digest.DigestUtils._
import org.apache.spark.HashPartitioner
import java.security.MessageDigest
import org.apache.spark.rdd.RDD.rddToOrderedRDDFunctions
import scala.Iterator


object SparkListenerTest extends App {
  
var recordsWrittenCount = 0L
var outputRecordWritten = 0L

val spark = SparkSession
					.builder()
					.appName("Spark SQL basic example").master("local")
					.getOrCreate()
					
    spark.sqlContext.setConf("spark.sql.shuffle.partitions", "2")
    
import spark.implicits._

/**http://javarevisited.blogspot.in/2013/06/how-to-generate-md5-checksum-for-files.html
 * http://technology.finra.org/code/using-spark-transformations-for-mpreduce-jobs.html
 * https://dzone.com/articles/a-complete-introduction-to-kubernetes-an-orchestra*/

  def calculateCheckSum(rdd:RDD[String], parts: Int = 1): String = {
    val partitioner = new HashPartitioner(parts)
    val output = rdd.map(x => (x, 1))
      .repartitionAndSortWithinPartitions(partitioner)
      .map(x => {println(x._1);(x._1)})
      .mapPartitions(x => Iterator(x.foldLeft(getMd5Digest())(md5)))
      .map(x =>  String.format("%032x",new java.math.BigInteger(1, x.digest())))    //new java.math.BigInteger(1, x.digest()).toString(16)) 
      //String.format("%032x",new BigInteger(1, hash))
      .collect()
      .sorted
      .foldLeft(getMd5Digest())(md5)
    val checksum = String.format("%032x",new java.math.BigInteger(1, output.digest()))
    return (checksum)
  }

 def md5(currMD5: MessageDigest, input: String): MessageDigest = {
    val inputBytes = input.getBytes("UTF-8")
    currMD5.update(inputBytes, 0, inputBytes.length)
    currMD5
  }

  val fullRdd = spark.sparkContext.textFile("D:\\caseclass.txt", 5)     //read.textFile("D:\\caseclass.txt").rdd
  println(calculateCheckSum(fullRdd))
}


/*val sc = spark.sparkContext

    val metrics = ArrayBuffer.empty[Long]
    var funcname = ""
    val listener = new QueryExecutionListener {
      override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {}

      override def onSuccess(funcName: String, qe: QueryExecution, duration: Long): Unit = {
        funcname += funcName + "," +qe.optimizedPlan.verboseStringWithSuffix
        val metric = qe.executedPlan match {
          case w: WholeStageCodegenExec => w.child.longMetric("numOutputRows")
          case other => other.longMetric("numOutputRows")
        }
        metrics += metric.value
      }
    }
    spark.listenerManager.register(listener)*/

/*sc.addSparkListener(new SparkListener() { 
  override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
    synchronized {
      recordsWrittenCount += taskEnd.taskInfo.accumulables(6).value.get.asInstanceOf[Long] 
      outputRecordWritten += taskEnd.taskMetrics.outputMetrics.recordsWritten
    }
  }
})*/
		/*val data1s = Range(0, 100000).map(x => (s"key${x}", x,null.asInstanceOf[Integer]))

    val df1 = spark.sparkContext.parallelize(data1s,10).toDF("key.[1]","value","value.[1]")
    val df2 = spark.sparkContext.parallelize(data1s,12).toDF("key.[1]","value1","value2")
    println(df1.rdd.getNumPartitions)
    df1.repartition(10)
    df2.repartition(10)*/
/*   println(s"post repartitioning: ${df1.rdd.getNumPartitions}")

    val joinedDF = df1.join(df2,Seq("key.[1]")) 
        
    joinedDF.show()
    
    println(joinedDF.explain(true))*/

    
    //df1.groupBy("value").count().collect()
    
    //df1.write.csv("/tmp/dfjoined2")
    
//sc.parallelize(1 to 10909, 2).saveAsTextFile("/tmp/foobar1")

/*    
    val schema = StructType(
    StructField("k", DateType, true) ::
    StructField("v", IntegerType, false) :: Nil)
    
    val date3 = new java.util.Date("1987-09-09")
    val date1 = new SimpleDateFormat("yyyyMMdd")
    val date2 = date1.parse("19870909")
    println(date2.toString())
    val date =  java.sql.Date.valueOf(date1.format(date3))  //new java.sql.Date(date2.getTime)
    val row = Row.fromSeq(Seq(date,89))
    val rows = spark.sparkContext.parallelize(Seq(row))
    val df = spark.createDataFrame(rows, schema)
    
    df.show()*/
  
