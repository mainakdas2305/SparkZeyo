package SparckPack

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import com.mysql.jdbc._

object SparkObjRevisionMainak {
  case class schema
(
		txnno:String,
		txndate:String,
		custno:String,
		amount:String,
		category:String,
		product:String,
		city:String,
		state:String,
		spendby:String
		)
def main(args:Array[String]):Unit={



		val conf= new SparkConf().setAppName("Spark").setMaster("local[*]") // vice president 

				val sc = new SparkContext(conf)   // president
				sc.setLogLevel("ERROR")



				val spark = SparkSession.builder().getOrCreate()
				import spark.implicits._



				println("===3 >>>Create a scala List with 1,4,6,7 and Do an iteration and add 2 to it===")

				val list= List(1,4,6,7)
				val add= list.map(x=>x+2)
				add.foreach(println)



				println("===4 >>>Create a scala List with zeyobron,zeyo and analytics and filter elements contains zeyo===")

				val list_string= List("zeyobron","zeyo","analytics")
				val filter=list_string.filter(x=>x.contains("zeyo"))
				filter.foreach(println)


				println("===5 >>>Read file1 as an rdd and filter gymnastics rows===")

				val fil1=sc.textFile("file:///C://data/datasetsRevision/file1.txt")
				val gymdata=fil1.filter(x=>x.contains("Gymnastics"))
				gymdata.take(20).foreach(println)


				println("===6 >>>Create a case class and impose case class to it and filter product contains Gymnastics===")

				val mapsplit=fil1.map(x=>x.split(","))
				val schemardd=mapsplit.map(x=>schema(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)))
				val gymprod=schemardd.filter(x=>x.product.contains("Gymnastics"))
				gymprod.take(10).foreach(println)


				println("===7 >>>Read file2 , convert it row rdd and  filter last index equals Cash===")

				val fil2=sc.textFile("file:///C://data/datasetsRevision/file2.txt")
				val msplit=fil2.map(x=>x.split(","))
				val rowrdd=msplit.map(x=>Row(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)))
				val cashdata=rowrdd.filter(x=>x(8).toString().contains("cash"))
				cashdata.take(10).foreach(println)



				println("===8 >>>Create dataframe using schema rdd and row rdd===")

				val schemadf=schemardd.toDF()
				schemadf.show()

				val simpleSchema = StructType(Array(
						StructField("txnno",StringType,true),
						StructField("txndate",StringType,true),
						StructField("custno",StringType,true),
						StructField("amount", StringType, true),
						StructField("category", StringType, true),
						StructField("product", StringType, true),
						StructField("city", StringType, true),
						StructField("state", StringType, true),
						StructField("spendby", StringType, true)
						))

				val rowdf=spark.createDataFrame(rowrdd, simpleSchema)
				rowdf.show()


				println("======9 >>>Read file 3 as csv with header true and Show===")

				val fil3=spark.read.format("csv").option("header","true").load("file:///C://data/datasetsRevision/file3.txt")
				fil3.show()



				println("======10 >>>Read file 4 as json and file 5 as parquet and show both the dataframe===")

				val fil4=spark.read.format("json").load("file:///C://data/datasetsRevision/file4.json")
				val fil5=spark.read.format("parquet").load("file:///C://data/datasetsRevision/file5.parquet")

				fil4.show()
				fil5.show()


				println("======11 >>>Read file 6 as xml with row tag as txndata===")

				val fil6=spark.read.format("com.databricks.spark.xml").option("rowTag", "txndata").load("file:///C://data/datasetsRevision/file6")
				fil6.show()




				println("======12 >>>Define a unified column list and impose using select and Union all the dataframes===")
				
				val collist= List("txnno","txndate","custno","amount","category","product","spendby","state","city")
				val uniondf=schemadf.select(collist.map(col):_*)
				.union(  rowdf.select(collist.map(col):_*)  )
				.union(  fil3.select(collist.map(col):_*)  )
				.union(  fil4.select(collist.map(col):_*)  )
				.union(  fil5.select(collist.map(col):_*)  )
				.union(  fil6.select(collist.map(col):_*)  )

				uniondf.show()
				
				
				
				println("======13 >>>Get year from txn date and rename it with year and add one column at the end as status 1 for cash and 0 for credit in spendby and filter txnno<50000===")
				
				val dsldf=uniondf
				.withColumn("txndate", expr("split(txndate,'-')[2]"))
				.withColumnRenamed("txndate", "year")
				.withColumn("status", expr("case when spendby='cash' then 1 else 0 end"))
				.filter(col("txnno")<50000)
				
				dsldf.show()
				
				
				
				println("======14 >>>Find the Cummulative sum of  amount, count of status foreach category===")
				
				val aggdf=dsldf.groupBy("category").agg(sum("amount").alias("sum_amount"),count("status").as("status_count"))
				aggdf.show()
				
				
				
				println("======15 >>>Write the cumulative  results  to RDBMS under txn db with table name as cummulative results with overwrite mode===")
				
				aggdf.write
				.format("jdbc")
				.option("url", "jdbc:mysql://mysql56.cki8jgd5zszv.ap-south-1.rds.amazonaws.com:3306/txn")
				.option("driver", "com.mysql.jdbc.Driver")
				.option("user", "root")
				.option("password", "Aditya908")
				.option("dbtable", "cummulative_results_mainak")
				.mode("overwrite")
				.save()
				println(" data written to mysql ")
				
				
				
				
				println("======16 >>>Write as an avro in local with mode append and partition the category column===")
				
				aggdf.write.format("com.databricks.spark.avro").mode("append").partitionBy("category").save("file:///C://data//write_avro_revision")
				





}
}