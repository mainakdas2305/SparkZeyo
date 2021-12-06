package SparckPack

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object SparkObjComplexJsonMiscJsonFiles {
  
  def main(args:Array[String]):Unit={



			val conf= new SparkConf().setAppName("Spark").setMaster("local[*]") // vice president 

					val sc = new SparkContext(conf)   // president
					sc.setLogLevel("ERROR")



					val spark = SparkSession.builder().getOrCreate()

					import spark.implicits._

					println("=======picture_multiline.json=======")		
					val pm_df= spark.read.format("json").option("multiLine", "true")
					.load("file:///C://data/picture_multiline.json")

					pm_df.show(false) 
					pm_df.printSchema()
					println("=======picture_multiline.json   flattened=======")			
					val pm_flatdf=pm_df.select("id",
							"image.*",
							"name",
							"thumbnail.*",
							"type")
					pm_flatdf.show(false) 
					pm_flatdf.printSchema()




					println("=======reqapi.json=======")		
					val ra_df= spark.read.format("json").option("multiLine", "true")
					.load("file:///C://data/reqapi.json")
					ra_df.show(false) 
					ra_df.printSchema()
					println("=======reqapi.json  flattened=======")	
					val ra_flatdf=ra_df.select(
							"data.*",
							"page",
							"per_page",
							"support.*",
							"total",
							"total_pages")
					ra_flatdf.show(false) 
					ra_flatdf.printSchema()





					println("=======pets.json=======")		
					val pets_df= spark.read.format("json").option("multiLine", "true")
					.load("file:///C://data/pets.json")
					pets_df.show(false) 
					pets_df.printSchema()



					println("=======pets.json  flattened=======")	
					val pets_flatdf=pets_df.withColumn("Pets", explode(col("Pets"))).select(
							"Address.*",
							"Boolean",
							"Name",
							"Pets")
					pets_flatdf.show(false) 
					pets_flatdf.printSchema()









					println("=======actors.json=======")		
					val actors_df= spark.read.format("json").option("multiLine", "true")
					.load("file:///C://data/actors.json")
					actors_df.show(false) 
					actors_df.printSchema()



					println("=======actors.json  flattened=======")	
					val actors_flatdf=actors_df
					.withColumn("Actors", explode(col("Actors")))
					.select(
							"Actors.*",
							"country",
							"version"
							)
					.withColumn("children", explode(col("children")))		

					actors_flatdf.show(false) 
					actors_flatdf.printSchema()





	}
}