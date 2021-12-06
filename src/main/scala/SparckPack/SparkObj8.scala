package SparckPack
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.Row

object SparkObj8 {
  def main(args:Array[String]):Unit={

      val conf= new SparkConf().setAppName("Spark").setMaster("local[*]")  
      val sc = new SparkContext(conf)   
			sc.setLogLevel("ERROR")

			println("=================raw data============")


val data = sc.textFile("file:///C:/data/txnsample.txt")
data.foreach(println)


println("==============Map Split=========")

val mapsplit=data.map(x=>x.split(","))



println("=============Row rdd==============")

val rowrdd = mapsplit.map(x=>Row(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)))
rowrdd.foreach(println)


println("==============row rdd filter=================")


val filterrowrdd = rowrdd.filter(x=>x(5).toString().contains("Gymnastics"))

filterrowrdd.foreach(println)

			
						
			
	}
}