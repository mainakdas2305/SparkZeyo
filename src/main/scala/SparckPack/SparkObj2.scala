package SparckPack

object SparkObj2 {
  def main(args:Array[String]):Unit={

      
			println("=====================raw list===================")


			val rawdata=List("State->TamilNadu~City->Chennai",
			                 "State->Karnataka~City->Bangalore",
			                 "State->Maharashtra~City->Mumbai")
			
			rawdata.foreach(println)

	
			println("====================flatten data=============")
			
			
			val flatdata = rawdata.flatMap( x =>  x.split("~"))
			
			flatdata.foreach(println)
			
			println("====================pick data=============")
      val states=flatdata.filter(x =>  x.contains("State->"))
      states.foreach(println)
      
      val cities=flatdata.filter(x =>  x.contains("City->"))
      cities.foreach(println)
      
      println("====================pick data=============")
      //val statelist=states.map(x =>  x.replace("State->",""))
      //statelist.foreach(println)
     // val citylist=cities.map(x =>  x.replace("City->",""))
     // citylist.foreach(println)
      val statesappend=states.map(x => x + "->state")
      statesappend.foreach(println)
      val citiesappend=cities.map(x => x + "->city")
      citiesappend.foreach(println)
      
       println("====================final list=============")
     // val finallist=List.concat(statelist,citylist)
     // finallist.foreach(println)
      
     // val uniondata=statelist.union(citylist)
			//uniondata.foreach(print)
      val stateflattened=statesappend.flatMap(x => x.split("->"))
      stateflattened.foreach(println)
      
      val cityflattened=citiesappend.flatMap(x => x.split("->"))
      cityflattened.foreach(println)
	}
}