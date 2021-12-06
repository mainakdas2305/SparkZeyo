package SparckPack

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object SparkObjComplexJsonData {
  def main(args:Array[String]):Unit={



			val conf= new SparkConf().setAppName("Spark").setMaster("local[*]") // vice president 

					val sc = new SparkContext(conf)   // president
					sc.setLogLevel("ERROR")



					val spark = SparkSession.builder().getOrCreate()

					import spark.implicits._

					println("=======complex2.json=======")		
					val df = spark.read.option("multiLine","true")
				.json("file:///C://data//complex2.json")
				df.show()
				df.printSchema()

				println("===========Explode array===========")//==================================
				val explodedf = df.withColumn("Students", explode(col("Students")))
				explodedf.show()
				explodedf.printSchema()
				println("===========flatten data===========")//==================================
				val flatdf = explodedf.select(
						"Students.user.*",
						"address.*",
						"orgname",
						"trainer"
						)
				flatdf.show()
				flatdf.printSchema()
				println("===========complex data===========")//==================================
				val compdf = flatdf.select(
						col("location"),
						col("name"),
						struct(
								col("permanent_address"),
								col("temporary_address")
								).alias("address"),
						col("orgname"),
						col("trainer")
						)
				.groupBy("orgname","trainer","address").agg(

						collect_list( 
								struct( 
										struct(
												"location",
												"name"
												).alias("user")  
										)  
								).alias("Students")
				)
						compdf.show()
						compdf.printSchema()

	}
}