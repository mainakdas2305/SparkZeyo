package SparckPack

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object SparkObjComplexJsonWebApi {
  
  def main(args:Array[String]):Unit={



			val conf= new SparkConf().setAppName("Spark").setMaster("local[*]") // vice president 

					val sc = new SparkContext(conf)   // president
					sc.setLogLevel("ERROR")



					val spark = SparkSession.builder().getOrCreate()

					import spark.implicits._
					println("===========web api data===========")
					val data = scala.io.Source
					.fromURL("https://randomuser.me/api/0.8/?results=10")
					.mkString
					val rdd =  sc.parallelize(List(data))
					val df = spark.read.json(rdd)

					df.show()
					df.printSchema()
					println("===========flattened data===========")
					val flatdf = df
					.withColumn("results",explode(col("results")))
					flatdf.show()
					flatdf.printSchema()
					println("===========final flattened data===========")
					val finalflat = flatdf
					.select(
							"nationality",
							"results.user.cell",
							"results.user.dob",
							"results.user.email",
							"results.user.gender",
							"results.user.location.city",
							"results.user.location.state",
							"results.user.location.street",
							"results.user.location.zip",
							"results.user.md5",
							"results.user.name.first",
							"results.user.name.last",
							"results.user.name.title",
							"results.user.password",
							"results.user.phone",
							"results.user.picture.large",
							"results.user.picture.medium",
							"results.user.picture.thumbnail",
							"results.user.registered",
							"results.user.salt",
							"results.user.sha1",
							"results.user.sha256",
							"results.user.username",
							"seed",
							"version"
							)


					finalflat.show(false)
					finalflat.printSchema()


					println("===========complex data===========")
					val compdf = finalflat.select(
							col("nationality"),
							struct(struct(
									col("cell"),
									col("dob"),
									col("email"),
									col("gender"),
									struct(
											col("city"),
											col("state"),
											col("street"),
											col("zip")		
											).alias("location"),
									col("md5"),
									struct(
											col("first"),
											col("last"),
											col("title")		
											).alias("name"),
									col("password"),
									col("phone"),
									struct(
											col("large"),
											col("medium"),
											col("thumbnail")		
											).alias("picture"),
									col("registered"),
									col("salt"),
									col("sha1"),
									col("sha256"),
									col("username")
									).alias("user")).alias("results"),
							col("seed"),
							col("version")
							)


					compdf.show()
					compdf.printSchema()

	}
}