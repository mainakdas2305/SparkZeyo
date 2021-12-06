package SparckPack
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
object SparkObj4 {
  def main (args:Array[String]):Unit={
    
    
    val conf=new SparkConf().setAppName("Spark").setMaster("local[*]")
    val sc=new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    println("==========raw data=========")
    
    val data=sc.textFile("file:///C:/data/usdata.csv")
     data.take(10).foreach(println)
     
     
     println("===========fil data=======")
     
     val fildata=data.filter(x=>x.length()>200)
     fildata.foreach(println)
     
    
    val flatdata=fildata.flatMap(x=>x.split(","))
    flatdata.foreach(println)
    
    val spacedata=flatdata.map(x=>x.replace(" ", ""))
    spacedata.foreach(println)
    
    
    val appenddata=spacedata.map(x=>x+",zeyo")
    appenddata.coalesce(1).saveAsTextFile("file:///C:/data/usdataprocessed")
    println("========data written=======")
  }
}