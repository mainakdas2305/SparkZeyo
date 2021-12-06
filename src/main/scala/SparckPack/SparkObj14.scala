package SparckPack


import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object SparkObj14 {
  def main(args:Array[String]):Unit={



		val conf= new SparkConf().setAppName("Spark").setMaster("local[*]") // vice president 

				val sc = new SparkContext(conf)   // president
				sc.setLogLevel("ERROR")



				val spark = SparkSession.builder().getOrCreate()
				import spark.implicits._


				println("=================raw data============")


				/*val data = sc.textFile("file:///C:/data/usdatawh.csv")
				data.take(5).foreach(println)*/
         
				println("=================seamless csv data============")
	      val seamlesscsv= spark.read.format("csv").load("file:///C:/data/usdata.csv")
	      seamlesscsv.show()
	      
				println("=================seamless csv data with header============")
	      val seamlesscsvhead= spark.read.format("csv").option("header","true").load("file:///C:/data/usdata.csv")
	      seamlesscsvhead.show()
	      
	      println("=================seamless parquet data============")
	      val seamlessparquetdata=spark.read.format("parquet").load("file:///C:/data/part_par.parquet")
	      seamlessparquetdata.show()
	      
	      println("=================seamless orc data============")
	      val seamlessorcdata=spark.read.format("orc").load("file:///C:/data/part_orc.orc")
	      seamlessorcdata.show()

				/*println("==============Map Split=========")

				val mapsplit=data.map(x=>x.split(","))


				println("=================row rdd======================")


				val rowrdd = mapsplit.map(x=> Row(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10),x(11),x(12)))



				println("=============define struct type=============")


				val structschema = StructType(Array(
						StructField("first_name",StringType,true),
						StructField("last_name",StringType,true),
						StructField("company_name",StringType,true),
						StructField("address", StringType, true),
						StructField("city", StringType, true),
						StructField("county", StringType, true),
						StructField("state", StringType, true),
						StructField("zip", StringType, true),
						StructField("age", StringType, true),
						StructField("phone1", StringType, true),
						StructField("phone2", StringType, true),
						StructField("email", StringType, true),
						StructField("web", StringType, true)
						))



			val df = spark.createDataFrame(rowrdd, structschema)
			
			println("=============dataframe raw=============")


			df.show(false)
			
			println("=============converting dataframe to rdd and print=============")
			//val rows: RDD[Row] = df.rdd
			val rows: org.apache.spark.rdd.RDD[org.apache.spark.sql.Row] = df.rdd
			rows.take(10).foreach(println)
			*/
			
}
}