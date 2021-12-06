package SparckPack

object SparkObj {
  def main(args:Array[String]):Unit={

      
			println("--- RAW DATA  ---")
			val data=List(10,20,30,40)
			data.foreach(println)
			
			println("--- MULTIPLY WITH 10 ---")
			val muldata=data.map(x => x*10)
			muldata.foreach(println)
			
			println("--- DIVIDE BY 5 ---")
			val dividedata=muldata.map(x => x/5)
			dividedata.foreach(println)
			
			println("--- FILTER GREATER THAN 50 ---")
			val fildata=dividedata.filter(x => x>50)
			fildata.foreach(println)
			
			println("--- RAW DATA  ---")
			val rawdata= List("zeyobron","analytics","zeyo")
			rawdata.foreach(println)
			
			println("--- FILTER DATA  ---")
			val fildata2=rawdata.filter(x => x.contains("zeyo"))
			fildata2.foreach(println)
			
			println("--- MAP DATA  ---")
			val mapdata=rawdata.map(x => x + ", Mainak")
			mapdata.foreach(println)
			
			println("--- MAP DATA  ---")
			val mapdata2=mapdata.map(x => x.replace("Mainak","Das"))
			mapdata2.foreach(println)
			
			
	}
}