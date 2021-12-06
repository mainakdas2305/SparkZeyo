package SparckPack

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object SparkObj10 {
  case class schema(c0:String,c1:String,c2:String,c3:String,c4:String,c5:String,c6:String,c7:String,c8:String)


def main(args:Array[String]):Unit={



		val conf= new SparkConf().setAppName("Spark").setMaster("local[*]") // vice president 

				val sc = new SparkContext(conf)   // president
				sc.setLogLevel("ERROR")



				val spark = SparkSession.builder().getOrCreate()
				import spark.implicits._


				println("=================raw data============")


				val data = sc.textFile("file:///C:/data/txnsample.txt")
				data.foreach(println)


				println("==============Map Split=========")

				val mapsplit=data.map(x=>x.split(","))


				println("=================row rdd======================")


				val rowrdd = mapsplit.map(x=> Row(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)))



				println("=============define struct type=============")


				val structschema = StructType(Array(
						StructField("c0",StringType,true),
						StructField("c1",StringType,true),
						StructField("c2",StringType,true),
						StructField("c3", StringType, true),
						StructField("c4", StringType, true),
						StructField("c5", StringType, true),
						StructField("c6", StringType, true),
						StructField("c7", StringType, true),
						StructField("c8", StringType, true)
						))



			val df = spark.createDataFrame(rowrdd, structschema)
			
			println("=============dataframe raw=============")


			df.show()
			
			df.createOrReplaceTempView("txndf")
			
			println("=============Processed dataframe=============")


			
			val resultantdf= spark.sql("select * from txndf where c5 like '%Gymnastics%'")
			resultantdf.show()
			


}
}