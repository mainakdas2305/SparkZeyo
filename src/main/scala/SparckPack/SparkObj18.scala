package SparckPack
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
object SparkObj18 {
  def main(args:Array[String]):Unit={



			val conf= new SparkConf().setAppName("Spark").setMaster("local[*]") // vice president 

					val sc = new SparkContext(conf)   // president
					sc.setLogLevel("ERROR")



					val spark = SparkSession.builder().getOrCreate()
					import spark.implicits._




					println("=================mode check============")
					val df= spark.read.format("csv").option("header","true").load("file:///C:/data/usdata.csv")
					df.show()
					//val dfwritemode=df.write.format("parquet").save("file:///C:/data/data_write_mode_check")
					//val dfwritemode=df.write.format("parquet").mode("error").save("file:///C:/data/data_write_mode_check")
					//val dfwritemode=df.write.format("parquet").mode("append").save("file:///C:/data/data_write_mode_check")
					//val dfwritemode=df.write.format("parquet").mode("overwrite").save("file:///C:/data/data_write_mode_check")
					//val dfwritemode=df.write.format("parquet").mode("ignore").save("file:///C:/data/data_write_mode_check")
	}

}