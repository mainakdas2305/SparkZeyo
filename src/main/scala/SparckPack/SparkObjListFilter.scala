package SparckPack

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object SparkObjListFilter {
  def main(args:Array[String]):Unit={



			val conf= new SparkConf().setAppName("Spark").setMaster("local[*]") // vice president 

					val sc = new SparkContext(conf)   // president
					sc.setLogLevel("ERROR")



					val spark = SparkSession.builder().getOrCreate()
					import spark.implicits._




					
					val df= spark.read.format("csv").option("header","true").load("file:///C:/data/file1.csv")
					df.show(false)


					val df2= spark.read.format("csv").option("header","true").load("file:///C:/data/file2.csv")
					df2.show()


					val listValues=df2.select("id").map(f=>f.getString(0))
					.collect.toList
					println(listValues)

					val filterdf=df.filter(!col("id").isin(listValues:_*))

					filterdf.show()


					//df.filter(($"id".equalTo(list))).show()
	}

}