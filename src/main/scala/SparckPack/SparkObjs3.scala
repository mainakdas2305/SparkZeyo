package SparckPack

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object SparkObjs3 {
  def main(args:Array[String]):Unit={



			val conf= new SparkConf().setAppName("Spark").setMaster("local[*]") // vice president 

					val sc = new SparkContext(conf)   // president
					sc.setLogLevel("ERROR")



					val spark = SparkSession.builder()
					.config("fs.s3a.access.key","AKIAQJ35YRX5GAI3I3WR")
					.config("fs.s3a.secret.key","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")
					.getOrCreate()

					import spark.implicits._


					val df= spark.read.format("csv").option("header","true")
					.load("s3a://zeyonifibucket/srcdir/usdata.csv")

					df.show() 

					df.write.format("com.databricks.spark.avro").mode("overwrite")
					.save("s3a://zeyonifibucket/tardir/mainak_write")

					println("Done")



	}
}