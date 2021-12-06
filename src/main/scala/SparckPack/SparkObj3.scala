package SparckPack
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
object SparkObj3 {
  def main (args:Array[String]):Unit={
    
    
    val conf=new SparkConf().setAppName("Spark").setMaster("local[*]")
    val sc=new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    println("==========raw data=========")
    
    val data=sc.textFile("file:///C:/data/txns")
     data.take(10).foreach(println)
     
     
     println("===========fil data=======")
     
     val gymdata=data.filter(x=>x.contains("Gymnastics"))
     gymdata.take(10).foreach(println)
     
    
    gymdata.coalesce(1).saveAsTextFile("file:///C:/data/gymdataprocessed")
    println("=======data written============")
  }
}