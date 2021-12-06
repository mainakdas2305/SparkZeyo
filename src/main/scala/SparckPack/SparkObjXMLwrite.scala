package SparckPack

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


object SparkObjXMLwrite {
  def main(args:Array[String]):Unit={



			val conf= new SparkConf().setAppName("Spark").setMaster("local[*]") // vice president 

					val sc = new SparkContext(conf)   // president
					sc.setLogLevel("ERROR")



					val spark = SparkSession.builder().getOrCreate()
					import spark.implicits._




					val df= spark.read.format("csv").option("header","true")
				.load("file:///C:/data/usdata.csv")
				
				df.show()
				
				df.write.format("com.databricks.spark.xml").option("rowTag","usdata").mode("overwrite")
				.save("file:///C:/data/xmlwritespark")



	}
}