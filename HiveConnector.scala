package tickdata

import java.sql.Connection
import java.sql.DriverManager
import org.apache.spark.sql.hive
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.orc._
import org.apache.spark.sql._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.hive.jdbc.HiveDatabaseMetaData
import org.apache.hadoop.hive.thrift
import org.apache.spark.sql.hive
import org.joda.time.DateTime
import org.joda.time.{ DateTime, Period }
import java.sql.Timestamp
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.sql.Timestamp
import scala.language.implicitConversions
import org.apache.spark.streaming.Interval
import java.util.List
import org.apache.commons.math3.stat.Frequency
import scala.collection.script.Start
import com.sun.xml.internal.ws.org.objectweb.asm.Item
import groovy.sql.Sql
import org.apache.spark.sql.types._
import org.apache.spark.sql.hive.HiveContext
import org.apache.slider.server.appmaster.management.Timestamp
import org.apache.spark.sql.functions
import org.joda.time.format.DateTimeFormat
import org.apache.spark.sql.columnar.LONG
import scala.collection.immutable.Map
import org.apache.hadoop.fs.DF
import org.apache.hadoop
import java.text.SimpleDateFormat
import org.apache.spark.sql.columnar.LONG
import java.util.{Date, Locale}
import java.text.DateFormat
import java.text.DateFormat._
import org.apache.hadoop.hive.ql.exec.UDF
import org.apache.hive.service.cli.HiveSQLException
import org.apache.hadoop.hive.llap.counters.QueryFragmentCounters.Desc
import org.apache.hadoop.hive.ql.plan.DescDatabaseDesc
import java.time.LocalDate
import scala.collection.mutable.ArrayBuffer
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.OL

object HiveConnector {

	def main(args: Array[String]) {

		// pour desactiver les logs par defaut de spark (org, akka)
		Logger.getLogger("org").setLevel(Level.OFF)
		Logger.getLogger("akka").setLevel(Level.OFF)
		// instanciation de la class MarketDept
		val textConnect = new MarketDepthServices()
		//textConnect.openConnect
		// consultation du marketDeph
		textConnect.consultation("20160427-09:29", "20160427-09:30")


	}     
}
