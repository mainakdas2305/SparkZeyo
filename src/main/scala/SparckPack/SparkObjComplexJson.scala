package SparckPack
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
object SparkObjComplexJson {
  
	def main(args:Array[String]):Unit={



			val conf= new SparkConf().setAppName("Spark").setMaster("local[*]") // vice president 

					val sc = new SparkContext(conf)   // president
					sc.setLogLevel("ERROR")



					val spark = SparkSession.builder().getOrCreate()

					import spark.implicits._


					val df= spark.read.format("json").option("multiLine", "true")
					.load("file:///C://data/complex1.json")

					df.show() 
					df.printSchema()
					
					
					val flatdf=df.select("orgname",
					                      "trainer",
					                      "address.permanent_address",
					                      "address.temporary_address")
					                      
					                      
					println ("======flattened dataframe=====")
					flatdf.show()
					flatdf.printSchema()



					println("======complex df===")

					val comdf=flatdf
					.select(col("orgname"),
							col("trainer"),
							struct(

									col("permanent_address"),
									col("temporary_address")


									).alias("address_new")
							)

					comdf.show()
					comdf.printSchema()
					
				




	}

}