package tickdata

import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import scala.collection.immutable.Nil
import org.apache.slider.server.appmaster.management.Timestamp
import org.apache.hadoop.hive.llap.Row
import java.lang.reflect.Array


class MarketDataService (){

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
	val data = hivecontext.sql("SELECT * FROM " + Database.trim() + "." + Table.trim())

	/*methode pour filter par ASK
	def getAsk {
				//val ask_bid = data.filter("c4='A'").show()
				data.filter("c4='A'").show()	                      
	}
	//  methode pour filter par Bid 
	def getBid {
				data.filter("c4='B'").show()		
	}
		 // pour joindre les 2 filtres 
	 def consultation{
	    //data.filter("c0='20160427-09:29:02.567'").show()  
	}
   */
   
	
}
