package SparckPack


import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object SparkObj17 {
  def main(args:Array[String]):Unit={



			val conf= new SparkConf().setAppName("Spark").setMaster("local[*]") // vice president 

					val sc = new SparkContext(conf)   // president
					sc.setLogLevel("ERROR")



					val spark = SparkSession.builder().getOrCreate()
					import spark.implicits._




					println("=================json read check============")
					val jsondf = spark.read.option("multiline","true").json("file:///C:/data/randomeuser5.json")
					jsondf.show(false) 
					println("=================json schema print============")
					jsondf.printSchema();
	}
}