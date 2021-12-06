package SparckPack

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object SparkObjComplexJson2 {
  
  def main(args:Array[String]):Unit={



			val conf= new SparkConf().setAppName("Spark").setMaster("local[*]") // vice president 

					val sc = new SparkContext(conf)   // president
					sc.setLogLevel("ERROR")



					val spark = SparkSession.builder().getOrCreate()

					import spark.implicits._

					println ("=======handling array=====")
					val df= spark.read.format("json").option("multiLine", "true")
					.load("file:///C://data/file1.json")

					df.show(false) 
					df.printSchema()
					println ("=======applying .* and handling struct=====")
					val flatdf1=df.select("Students",
							"address.*",
							"orgname",
							"trainer")

					flatdf1.show(false) 
					flatdf1.printSchema()  
					println ("=======applying explode and handling array=====")
					val flatdf2=flatdf1.withColumn("Students", explode(col("Students")))
					flatdf2.show(false) 
					flatdf2.printSchema()  





					println ("=======handling struct under array=====")
					val df2= spark.read.format("json").option("multiLine", "true")
					.load("file:///C://data/file2.json")
					df2.show(false) 
					df2.printSchema()
					println ("=======applying explode and handling array=====")
					val flatdata1_2=df2.withColumn("Students",  explode(col("Students")))
					flatdata1_2.show(false) 
					flatdata1_2.printSchema()
					println ("=======applying .* and handling struct=====")
					val finalflat=flatdata1_2.select("Students.*",
							"address.*",
							"orgname",
							"trainer")

					finalflat.show(false)
					finalflat.printSchema()

	}

}