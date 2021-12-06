package SparckPack
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


object SparkObj6 {
  case class schema(txnno:String,
      txndate:String,
      custno:String,
      amount:String,
      category:String,
      product:String,
      city:String,
      state:String,
      spendby:String)


	def main(args:Array[String]):Unit={

      val conf= new SparkConf().setAppName("Spark").setMaster("local[*]")  
      val sc = new SparkContext(conf)   
			sc.setLogLevel("ERROR")

			println("=================raw data============")


			val data = sc.textFile("file:///C:/data/txnduplicate.txt")
			data.take(10).foreach(println)
			
				println("=================distinct data============")

			val distinct=data.distinct()
			distinct.foreach(println)
	}
}