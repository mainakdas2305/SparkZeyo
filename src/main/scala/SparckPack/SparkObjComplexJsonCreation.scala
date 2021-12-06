package SparckPack

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


object SparkObjComplexJsonCreation {
  def main(args:Array[String]):Unit={



			val conf= new SparkConf().setAppName("Spark").setMaster("local[*]") // vice president 

					val sc = new SparkContext(conf)   // president
					sc.setLogLevel("ERROR")



					val spark = SparkSession.builder().getOrCreate()

					import spark.implicits._

					println("=======complex2.json=======")		
					val df= spark.read.format("json").option("multiLine", "true")
					.load("file:///C://data/complex2.json")

					df.show(false) 
					df.printSchema()
					
					
					println("=======complex2.json   flattened=======")			
					val explodedf=df.withColumn("Students", explode(col("Students")))
					explodedf.show()
					explodedf.printSchema()
					
					val flatdf=explodedf.select("Students",
							"address.*",
							"orgname",
							"trainer")
flatdf.show()
flatdf.printSchema()

val structdf=flatdf.select(
col("Students"),
struct(
    col("permanent_address"),
    col("temporary_address")).alias("address"),
    col("orgname"),
    col("trainer")
)

structdf.show()
structdf.printSchema()


val compdf=structdf.groupBy("address", "orgname","trainer")
.agg(collect_list("Students").alias("Students"))

compdf.show()
compdf.printSchema()




	}
}