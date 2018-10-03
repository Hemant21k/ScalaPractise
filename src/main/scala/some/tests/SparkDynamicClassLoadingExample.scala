package some.tests

import org.apache.spark.sql.{ Dataset, DataFrame, SparkSession }
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
 * https://github.com/kostaskougios/scalascriptengine/blob/master/src/main/scala/com/googlecode/scalascriptengine/Constructors.scala
 * https://stackoverflow.com/questions/49491968/spark-set-executors-class-loader
 * https://techblog.applift.com/upgrading-spark
 * http://apache-spark-developers-list.1001551.n3.nabble.com/Calling-external-classes-added-by-sc-addJar-needs-to-be-through-reflection-td6620.html
 * https://stackoverflow.com/questions/7452411/thread-currentthread-setcontextclassloader-without-using-reflection
 * http://blog.cask.co/2015/08/java-class-loading-and-distributed-data-processing-frameworks/
 * https://ardoris.wordpress.com/2014/03/30/how-spark-does-class-loading/
 * 
 */

object SparkDynamicClassLoadingExample {

  def transformDataFrame1(spark: SparkSession, df: DataFrame): DataFrame = {
    val delimiter = "/"
    val schema = StructType(Seq(StructField("name", StringType, true)))
    import spark.implicits._

    df.mapPartitions { partitions =>
      {
        val loader = Thread.currentThread.getContextClassLoader
        val CSVRecordParser = loader.loadClass("com.alpine.hadoop.ext.CSVRecordParser")
        val csvParser = CSVRecordParser.getConstructor(Character.TYPE)
          .newInstance(delimiter.charAt(0).asInstanceOf[Character])

        val parseLine = CSVRecordParser.getDeclaredMethod("parseLine", classOf[List[String]]) //getDeclaredMethod("parseLine", classOf[String]) 

        partitions.map { row => parseLine.invoke(row).asInstanceOf[Row]
        }

      }
    }(RowEncoder(schema)).toDF()
  }
  
  def transformDataFrame(df:DataFrame,spark:SparkSession)={
    val loader = Thread.currentThread.getContextClassLoader
    val dynamicClass = loader.loadClass("some.tests.SomeSparkTransformation")
    val transformU = dynamicClass.getConstructor().newInstance()
    val method = dynamicClass.getDeclaredMethod("trasformDF", classOf[DataFrame],classOf[SparkSession])
    method.invoke(transformU,df, spark)
    
  }
  

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")//.master("local")
      .getOrCreate()

    import spark.implicits._

    val df1 = Seq((1, "ty", 34, 45), (1, "ty", 34, 45), (2, "rt", 45, 67), (2, "rt1", 90, 87)).toDF("key", "name", "value", "value1")

    //df1.select($"key").show()
    val op_DF=transformDataFrame(df1,spark).asInstanceOf[DataFrame]
    //op_DF.show()
    op_DF.write.parquet(args(0))

  }

}