package SparckPack
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
object SparkObj5 {
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


			val data = sc.textFile("file:///C:/data/txns")
			data.take(10).foreach(println)
			
			
			println("=================filter data============")
			val gymdata=data.filter(x => x.contains("Gymnastics"))
			gymdata.take(10).foreach(println)
			val teamsportsdata=data.filter(x => x.contains("Team Sports"))
			teamsportsdata.take(10).foreach(println)
			
			
			println("=================union data============")
			val uniondata=gymdata.union(teamsportsdata)
			uniondata.take(10).foreach(println)
			
			
			
			uniondata.coalesce(1).saveAsTextFile("file:///C:/data/gym_and_team_sports_data_processed")
			println("=================written data============")
	}
}