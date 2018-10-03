package some.tests

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.Callable
import java.util.concurrent.FutureTask
import scala.util.Try
import java.util.concurrent.TimeUnit
import scala.util.Failure
import scala.util.Success

object MutiThreadedDriver {

  def runJob(spark: SparkSession) = {

    val df = spark.range(0, 100000000).toDF("key");
    
    
    val executorService = Executors.newFixedThreadPool(2);

    val future1 = new FutureTask[DataFrame](new Callable[DataFrame] {
      override def call(): DataFrame = {
      spark.sparkContext.setLocalProperty("spark.scheduler.pool", "fair_pool")
      try{
      val df1 = df.withColumn("key1",col("key")*4 ).join(df,Seq("key"))
      df1.write.parquet("D://multi1");
      df
      }
      catch{
        case e => throw new Exception(e.getMessage);
      }
      }
    })
    
   executorService.submit(future1);
    
   println("submitted future1");
    
    val future2 = new FutureTask[DataFrame](new Callable[DataFrame] {
      override def call(): DataFrame = {
      spark.sparkContext.setLocalProperty("spark.scheduler.pool", "fair_pool")
      val df2 = df.withColumn("key1",col("key")*2 ).join(df,Seq("key"))
      df2.write.parquet("D://multi2");
      df2
      }
    })
    
   executorService.submit(future2);
   

   println("submitted future2");
   
    
   val x = Try(future1.get())
   val y = Try(future2.get())
   
   if(x.isFailure || y.isFailure){
     future1.cancel(true)
     future2.cancel(true)
   }
   
    executorService.shutdownNow();

/*
    // Wait thread 1
    System.out.println("File1:" + x.isFailure)
    // Wait thread 2
    System.out.println("File2:" + y.isFailure) 
    
    */
/*          val df1 = df.withColumn("key1",col("key")*4 ).join(df,Seq("key"))
      df1.write.parquet("D://multi1");
          val df2 = df.withColumn("key1",col("key")*2 ).join(df,Seq("key"))
      df2.write.parquet("D://multi2");
             */
          
     }

  def main(args: Array[String]): Unit = {
  /**
   * https://github.com/dnvriend/apache-spark-test/blob/master/helloworld/src/main/scala/com/github/dnvriend/HelloWorld.scala
   * https://stackoverflow.com/questions/50407435/spark-opening-multiple-threads-for-a-single-job-while-trying-to-run-parallel-job
   * https://medium.com/@rbahaguejr/threaded-tasks-in-pyspark-jobs-d5279844dac0
   * http://www.russellspitzer.com/2017/02/27/Concurrency-In-Spark/ -- very imp
   * https://stackoverflow.com/questions/28712420/how-to-run-concurrent-jobsactions-in-apache-spark-using-single-spark-context/28719484#28719484
   */
    val spark = SparkSession.builder().config("spark.scheduler.mode","FAIR").config("spark.scheduler.allocation.file","D://schedulerconfig.xml").
    master("local[*]").appName("multi-threaded app").getOrCreate()
    //val spark = SparkSession.builder().master("local[*]").appName("multi-threaded app").getOrCreate()
    //spark.conf.set("spark.scheduler.mode","FAIR")
    runJob(spark);
  }
}