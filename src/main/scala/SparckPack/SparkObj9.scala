package SparckPack

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import org.apache.spark.sql.SparkSession

object SparkObj9 {
  case class schema(c0:String,c1:String,c2:String,c3:String,c4:String,c5:String,c6:String,c7:String,c8:String)


	def main(args:Array[String]):Unit={



			val conf= new SparkConf().setAppName("Spark").setMaster("local[*]") // vice president 

			val sc = new SparkContext(conf)   // president
			sc.setLogLevel("ERROR")

			println("=================raw data============")


			val data = sc.textFile("file:///C:/data/txnsample.txt")
			data.foreach(println)
			
			
			println("==============map split================")
			
			val mapsplit = data.map( x => x.split(","))
			
			
			println("=============schema rdd==============")
				
      val schemardd = mapsplit.map(x=>schema(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)))
      
      
			val spark = SparkSession.builder().getOrCreate()
      import spark.implicits._
      
      val df = schemardd.toDF()
      df.show(false)		

				
      df.createOrReplaceTempView("txndf")
				
				
      val resultdf =spark.sql("select * from txndf where c5 like '%Gymnastics%'")
				
      resultdf.show()
			
			
			
	}

}