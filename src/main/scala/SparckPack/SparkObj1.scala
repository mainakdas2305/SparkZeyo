package SparckPack

object SparkObj1 {
  def main(args:Array[String]):Unit={

      
			println("=====================rawdata===================")


			val rawdata=List("zeyobron~analytics~zeyo","sai~aditya")
			
			rawdata.foreach(println)

	
			println("====================flatten data=============")
			
			
			val flatdata = rawdata.flatMap( x =>  x.split("~"))
			
			flatdata.foreach(println)

			
	}
}