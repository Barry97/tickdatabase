package tickdata

import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import java.util.Properties
import org.apache.ivy.core.module.descriptor.Configuration
import org.apache.spark.sql.hive.orc._
//import java.sql.Date

import com.lmax.disruptor.YieldingWaitStrategy
import java.sql.Statement
import java.util.Date
import java.text.DateFormat
import org.json4s.DateFormat
import org.apache.orc.impl.ConvertTreeReaderFactory.DateFromTimestampTreeReader
import org.apache.spark.sql.catalyst.expressions.ToDate
import org.apache.spark.sql.catalyst.expressions.UnixTimestamp
import jdk.nashorn.internal.objects.annotations.Where

case class  MarketDepthServices(){

	val conf = new SparkConf().setAppName("Simple Application")
			.setMaster("local[*]")
			val sc = new SparkContext(conf)
	val hivecontext = new HiveContext(sc)
	import hivecontext.implicits._
	// nom de la base de données
	val Database = "eag_fxticks"
	//nom de la table 
	val Table = "mydata"
	// Methode pour la connection a la table Hive
	//		val data = hivecontext.sql("SELECT * FROM " + Database.trim() + "." + Table.trim() )
	val data = hivecontext.sql("SELECT c0,c1,c4,c5 FROM " + Database.trim() + "." + Table.trim())
	// openconnect
	
 	def openConnect {  
		data.show()
	}
	
	// Methode pour filtrer par ccy
	def setccy(ccy : String) {
		data.filter(ccy)
	}
	
	//  methode pour filter par Bid 
	def getBid {
		data.filter("c4='B'")
		.show()		
	}
	//  methode pour filter par Ask
	def getAsk{  
		data.filter("c4='A'")
		.show()
	}	
	
	def consultation(debut : String, fin : String){

		// filtre  entre date de debut et date de fin 
				val filterdData = data
				.filter($"c0".between(debut, fin)).as("filtre Date")
				// filtre par Ask	
				val ask = filterdData.filter("c4='B'").as("B")
			//	filtre par Bid
				val bid = filterdData.filter("c4='A'").as("A")
				//calcul du spread
				val bid_ask = ask.join(bid).withColumn("spread",$"A.c5" - $"B.c5").as("bidAsk").show()		
			
	}
	
}







