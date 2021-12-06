package SparckPack

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object SparkObjProject {
  def main(args:Array[String]):Unit={



			val conf= new SparkConf().setAppName("Spark").setMaster("local[*]") // vice president 

					val sc = new SparkContext(conf)   // president
					sc.setLogLevel("ERROR")



					val spark = SparkSession.builder().getOrCreate()

					import spark.implicits._
					
					println("================avro read===================")
					val avroread=spark.read.format("com.databricks.spark.avro").load("file:///C:/data/projectsample.avro")
					avroread.show(false)
          
					
					
					println("===========web api data===========")
					val data = scala.io.Source
					.fromURL("https://randomuser.me/api/0.8/?results=10")
					.mkString
					val rdd =  sc.parallelize(List(data))
					val df = spark.read.json(rdd)

					df.show()
					//df.printSchema()
					println("===========flattened data===========")
					val flatdf = df
					.withColumn("results",explode(col("results")))
					//flatdf.show()
					//flatdf.printSchema()
					
					
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
					//finalflat.printSchema()
					
					
			println("====== removed numericals from username ======")	
       val rmdf=  finalflat.withColumn("username" ,regexp_replace(col("username"),  "([0-9])", ""))
       rmdf.show()
       //rmdf.printSchema()
       
       
      println("====== broadcast left join ======")
      
			val join=		avroread.join(broadcast(rmdf),Seq("username"),"left") // have to ask why we are giving this left.why it is not working without left and giving blank table
      join.show()

      
       println("====== available customer dataframe ======")
       
      val notnulldf=join.filter(col("nationality").isNotNull)
      notnulldf.show()
      //notnulldf.printSchema()
      println("====== not available customer dataframe ======")
      val nulldf=join.filter(col("nationality").isNull)
      nulldf.show()
      //nulldf.printSchema()
      
       println("====== replace string column as 'Not Available' and integer column as '0'  ======")
      nulldf.na.fill(0).na.fill("Not Available").show() //have to ask how it is deciding the int and string values without specifying
      
      
      println("====== adding a new current_date column in available dataframe======")
      notnulldf.withColumn("current_date", current_date).show()
      
       println("====== adding a new current_date column in the not available dataframe======")
      nulldf.withColumn("current_date", current_date).show()

	}

}