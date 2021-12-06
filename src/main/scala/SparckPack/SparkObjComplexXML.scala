package SparckPack

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object SparkObjComplexXML {
  def main(args:Array[String]):Unit={



			val conf= new SparkConf().setAppName("Spark").setMaster("local[*]") // vice president 

					val sc = new SparkContext(conf)   // president
					sc.setLogLevel("ERROR")



					val spark = SparkSession.builder().getOrCreate()
					import spark.implicits._




					println("=================complex xml read ============")
					val xmldf= spark.read.format("com.databricks.spark.xml").option("rowTag","POSLog").load("file:///C:/data/transactions.xml")
					xmldf.show()
					xmldf.printSchema()



					println("===========flatten data===========")
					val flatdf = xmldf
					.withColumn("Transaction", explode(col("Transaction")))
					.withColumn("LineItem", explode(col("Transaction.RetailTransaction.LineItem")))
					.withColumn("LineItem", explode(col("Transaction.RetailTransaction.LineItem")))
					.withColumn("Total", explode(col("Transaction.RetailTransaction.Total")))

					.select(
							col("Transaction.BusinessDayDate"),
							col("Transaction.ControlTransaction.OperatorSignOff.*"),
							col("Transaction.ControlTransaction.ReasonCode"),
							col("Transaction.ControlTransaction._Version"),
							col("Transaction.CurrencyCode"),
							col("Transaction.EndDateTime"),
							col("Transaction.OperatorID.*"),
							col("Transaction.RetailStoreID"),
							col("Transaction.RetailTransaction.ItemCount"),
							col("LineItem.sale.Description"),
							col("LineItem.sale.DiscountAmount"),
							col("LineItem.sale.ExtendedAmount"),
							col("LineItem.sale.ExtendedDiscountAmount"),
							col("LineItem.sale.Itemizers.*"),
							col("LineItem.sale.MerchandiseHierarchy.*"),
							col("LineItem.sale.OperatorSequence"),
							col("LineItem.sale.POSIdentity.*"),
							col("LineItem.sale.Quantity"),
							col("LineItem.sale.RegularSalesUnitPrice"),
							col("LineItem.sale.ReportCode"),
							col("LineItem.sale._ItemType"),
							col("LineItem.SequenceNumber"),
							col("LineItem.Tax.*"),
							col("LineItem.Tender.Amount"),
							col("LineItem.Tender.Authorization.*"), 
							col("LineItem.Tender.OperatorSequence"),
							col("LineItem.Tender.TenderID"),
							col("LineItem.Tender._TenderDescription"),
							col("LineItem.Tender._TenderType"),
							col("LineItem.Tender._TypeCode"),
							col("LineItem._EntryMethod"),
							col("LineItem._weightItem"),
							col("Total.*")

							)
					flatdf.show()
					flatdf.printSchema()



	}

}