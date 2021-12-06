package SparckPack

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object SparkObj15 {
  def main(args:Array[String]):Unit={



		val conf= new SparkConf().setAppName("Spark").setMaster("local[*]") // vice president 

				val sc = new SparkContext(conf)   // president
				sc.setLogLevel("ERROR")



				val spark = SparkSession.builder().getOrCreate()
				import spark.implicits._


				

		    println("=================txt read parquet write============")
	      val seamlesstxt= spark.read.format("csv").option("header","true").option("delimiter","~").load("file:///C:/data/txnsamplepipe.txt")
	      seamlesstxt.show()
	      val parquetwrite=seamlesstxt.coalesce(1).write.format("parquet").mode("overwrite").save("file:///C:/data/parquetwrite")
	     
	      println("=================parquet read json write============")
	      val seamlessparquet= spark.read.format("parquet").load("file:///C:/data/parquetwrite")
	      seamlessparquet.show()
	      val jsonwrite=seamlessparquet.write.format("json").mode("overwrite").save("file:///C:/data/jsonwrite")
	      
	      println("=================json read orc write============")
	      val seamlessjson= spark.read.format("json").load("file:///C:/data/jsonwrite")
	      seamlessjson.show()
	      val orcwrite=seamlessjson.write.format("orc").mode("overwrite").save("file:///C:/data/orcwrite")
        
	      
	      println("=================orc read csv write============")
	      val seamlessorc= spark.read.format("orc").load("file:///C:/data/orcwrite")
	      seamlessorc.show()
        val csvwrite=seamlessorc.write.format("csv").option("header","true").option("delimiter","~").mode("overwrite").save("file:///C:/data/csvwrite")
        
        
	     
			
}
}