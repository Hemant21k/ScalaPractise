
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import java.io.File
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.{Dataset, DataFrame, SparkSession}
import java.net.URI
import org.apache.spark.SparkFiles

object ConfigParser {
  
  val spark = SparkSession
					.builder()
					.appName("Spark SQL basic example").master("local")
					.getOrCreate()
	val ctx = spark.sparkContext
	  
  case class employeeList(employee:Seq[employee])
  case class employee(employeename:String,employeedob:String)
  
  def readjson()={
/**https://stackoverflow.com/questions/29441316/specifying-an-external-configuration-file-for-apache-spark
 * https://docs.scala-lang.org/overviews/reflection/overview.html*/        
    val propertyFilePath = "hdfs:///user/hadoop/myproperties.properties"
    val fs = FileSystem.get(new URI(propertyFilePath), ctx.hadoopConfiguration)
    val inputStream = fs.open(new Path("hdfs:///user/hadoop/myproperties.properties"))
    
    val pt = new Path("hdfs:///user/hadoop/myproperties.properties");
    val fs1 = FileSystem.get(ctx.hadoopConfiguration);
    
        val mapper = new ObjectMapper with ScalaObjectMapper
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        mapper.registerModule(DefaultScalaModule)
        val json = new JsonFactory().createParser(new File("D:\\sample_json_files\\emp.json"));
        val hsdfsjson = new JsonFactory().createParser(inputStream.getWrappedStream)
        val it = mapper.readValue(json,classOf[employeeList])
        for(i <- it.employee){
          println(i.employeename)
        }
        json.close()

  }
  
  def metric[T](f: => T): T = {
    val t0 = System.nanoTime()
    val res = f
    println("Executed in " + (System.nanoTime()-t0)/1000000 + "ms")
    res
}
  
  def main(args:Array[String]):Unit ={
    readjson()
  }
}