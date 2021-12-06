package SparckPack

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object SparkObjComplexJsonReqResJson {
  def main(args:Array[String]):Unit={



			val conf= new SparkConf().setAppName("Spark").setMaster("local[*]") // vice president 

					val sc = new SparkContext(conf)   // president
					sc.setLogLevel("ERROR")



					val spark = SparkSession.builder().getOrCreate()

					import spark.implicits._

					println("=======reqres.json=======")		
					val df = spark.read.option("multiLine","true")
					.json("file:///C://data//reqres.json")
					df.show()
					df.printSchema()



					println("===========Explode array===========")//==================================
					val explodedf = df.withColumn("data", explode(col("data")))
					explodedf.show()
					explodedf.printSchema()

					println("===========flatten data===========")//==================================
					val flatdf = explodedf.select(
							"data.*",
							"page",
							"per_page",
							"support.*",
							"total",
							"total_pages"
							)
					flatdf.show()
					flatdf.printSchema()


					println("===========complex data===========")//==================================
					val compdf = flatdf.select(
							col("avatar"),
							col("email"),
							col("first_name"),
							col("id"),
							col("last_name"),
							col("page"),
							col("per_page"),
							struct(
									col("text"),
									col("url")
									).alias("support"),
							col("total"),
							col("total_pages")
							)
					.groupBy("page","per_page","support","total","total_pages").agg(

							collect_list( 
									struct(
											"avatar",
											"email",
											"first_name",
											"id",
											"last_name"
											)

									).alias("data")
							)
					compdf.show()
					compdf.printSchema()

	}

}