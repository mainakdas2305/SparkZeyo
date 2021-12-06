package SparckPack


import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object SparkObj21 {
  
  def main(args:Array[String]):Unit={



			val conf= new SparkConf().setAppName("Spark").setMaster("local[*]") // vice president 

					val sc = new SparkContext(conf)   // president
					sc.setLogLevel("ERROR")



					val spark = SparkSession.builder().getOrCreate()
					import spark.implicits._




					val df = spark.read.format("csv").option("header","true").load("file:///C:/data/txns_head")
					//df.show()
					df.persist()
					
					val filterdf= df.filter(col("category").isin("Gymnastics","Team Sports") && col("spendby")=!="credit")
					.withColumn("year_of_transaction",expr("date(from_unixtime(unix_timestamp(txndate,'MM-dd-yyyy')))"))
					.withColumnRenamed("txndate", "year")
					
					val fdf= df.groupBy("category")
				.agg(sum("amount").as("total_amount"),count("txnno").as("txn_count"))

					
					filterdf.show()
					fdf.show()
					
	}
}