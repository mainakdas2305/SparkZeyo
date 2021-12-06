package SparckPack


import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._


object SparkObj16 {
  def main(args:Array[String]):Unit={



			val conf= new SparkConf().setAppName("Spark").setMaster("local[*]") // vice president 

					val sc = new SparkContext(conf)   // president
					sc.setLogLevel("ERROR")



					val spark = SparkSession.builder().getOrCreate()
					import spark.implicits._




					println("=================avro read check============")
					val seamlessavro= spark.read.format("csv").option("header","true").load("file:///C:/data/usdata.csv")
					seamlessavro.show(false)
					val avrowrite=seamlessavro.write.format("com.databricks.spark.avro").mode("overwrite").save("file:///C:/data/csvavrowrite")
					println("================avro write===================")
					
					
					val avroread=spark.read.format("com.databricks.spark.avro").load("file:///C:/data/csvavrowrite")
					avroread.show(false)
          println("================avro read===================")
          
          println("=================xml read check============")
					val xmldf= spark.read.format("com.databricks.spark.xml").option("rowTag","book").load("file:///C:/data/book.xml")
			    xmldf.show()
			    val xmlavrowrite=xmldf.write.format("com.databricks.spark.avro").mode("overwrite").save("file:///C:/data/xmlavrowrite")
println("=================xml avro write============")

	}

}