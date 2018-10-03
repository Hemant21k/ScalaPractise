package some.tests

import org.apache.spark.sql.{Dataset, DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._


class SomeSparkTransformation extends Serializable {
  
  def trasformDF(df:DataFrame,spark:SparkSession)={
    import spark.implicits._
    df.select($"key")
  }
  
}