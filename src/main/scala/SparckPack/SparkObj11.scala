package SparckPack

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object SparkObj11 {
  case class schema(first_name:String,last_name:String,company_name:String,address:String,city:String,county:String,state:String,zip:String,age:String,phone1:String,phone2:String,email:String,web:String)


def main(args:Array[String]):Unit={



		val conf= new SparkConf().setAppName("Spark").setMaster("local[*]") // vice president 

				val sc = new SparkContext(conf)   // president
				sc.setLogLevel("ERROR")



				val spark = SparkSession.builder().getOrCreate()
				import spark.implicits._


				println("=================raw data============")


				val data = sc.textFile("file:///C:/data/usdatawh.csv")
				data.take(10).foreach(println)


				println("==============Map Split=========")

				val mapsplit=data.map(x=>x.split(","))

					
				println("=============schema rdd==============")

				val schemardd = mapsplit.map(x=>schema(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10),x(11),x(12)))



				println("=================dataframe================")


				val df = schemardd.toDF()
				df.show(false)


}
}