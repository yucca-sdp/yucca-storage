package org.csi.yucca.metadataalignment
import org.slf4j.LoggerFactory
import collection.immutable
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.hive._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import scala.collection.immutable
import scala.collection.mutable.Buffer
import org.joda.time.DateTime
import scopt._
import org.apache.spark.sql.DataFrame

object MetadataGovDashb_HiveCatalog {

  val LOG = LoggerFactory.getLogger(getClass)
  val zkHost: String = ""

  case class CliOptions(
      hiveMeta_url: String = "",
      hiveMeta_username: String = "",
      hiveMeta_password: String = "",
      timestamp: String = "",
      queue: String = "") {

  }

  val parser = new scopt.OptionParser[CliOptions]("ingest") {
    head("Connect to Hive Metastore ", "0.x")

    opt[String]("hiveMeta_url").required().action((hiveMeta_url, c) => { c.copy(hiveMeta_url = hiveMeta_url) })
      .text("Url Hive Metastore (eg: mysql://server/hivedb)")
    opt[String]("hiveMeta_username").required().action((hiveMeta_username, c) => { c.copy(hiveMeta_username = hiveMeta_username) })
      .text("Hive Metastore username")     
    opt[String]("hiveMeta_password").required().action((hiveMeta_password, c) => { c.copy(hiveMeta_password = hiveMeta_password) })
      .text("Hive Metastore password")
    opt[String]("timestamp").required().action((timestamp, c) => { c.copy(timestamp = timestamp) })
      .text("Timestamp in form 2018-10-14 11:41:26 ")
    opt[String]("queue").optional().action((queue, c) => { c.copy(queue = queue) })
      .text("Queue (optional, default=produzione)")
  }
  
def metaCatalog(db:String,mode:String,table_where:String,sqlContext:org.apache.spark.sql.SQLContext,sTabellaCat:String,sUrl:String,sUser:String,sPassword:String) ={
  
      
			var strSql=""
			var table_like=""
			if (table_where.contains("%")) table_like=s" like '$table_where'"
			if (table_where=="" ) table_like=s" like '%'"
			if (table_where.contains(",")) { 
  			val tt="'"+table_where.split(",").mkString("','")+"'"
	  		table_like=s" in (tt)"
			}
			
			strSql="(SELECT DB_ID, `DESC`, DB_LOCATION_URI, NAME, OWNER_NAME, OWNER_TYPE FROM hivedb.DBS) a"

			val reader = sqlContext.read.format("jdbc").options(Map("driver"->"com.mysql.jdbc.Driver","url" -> sUrl,"user"->sUser,"password"->sPassword))
      val df_hive_dbs=reader.option("dbtable", strSql).load

      //Non considero le tabelle external
			val strSql0="""(select a.DB_ID,DB_LOCATION_URI,NAME,OWNER_NAME, FROM_UNIXTIME(CREATE_TIME,'%Y-%m-%d %H.%i.%s') as CREATE_TIME, b.OWNER,
			b.TBL_ID,TBL_NAME,b.LAST_ACCESS_TIME,c.LOCATION
			from 
			hivedb.DBS a left join hivedb.TBLS b  on a.DB_ID=b.DB_ID
			left join hivedb.SDS c on b.SD_ID=c.SD_ID
			where TBL_TYPE <> 'EXTERNAL_TABLE'
			) a"""

			val df_hive_tbs=reader.option("dbtable", strSql0).load
			
			val strSql1="""(select a.*
			from hivedb.TABLE_PARAMS a) tb"""
			val df_hive_parms=reader.option("dbTable",strSql1).load

		  val df_piv=df_hive_parms.filter("param_key in ('numRows','transient_lastDdlTime','totalSize') ").groupBy("tbl_id").pivot("param_key").agg(max("param_value"))

			val df1=df_hive_dbs.as("db").join(df_hive_tbs.as("tb"),Seq("db_id")).join(df_piv.as("prm"),Seq("tbl_id"))

			val df2=df1.selectExpr("db.name as db_name","tb.tbl_name as tbl_name","tb.create_time as creation_date","tb.location as tb_location", "prm.numRows as num_rows","prm.totalSize as total_size").filter(s"db_name='$db' and tbl_name $table_like")

			if (mode=="overwrite" ) {
    			 df2.write.format("orc").mode("overwrite").saveAsTable(sTabellaCat)
					}
					else if (mode=="append" ) {
					 df2.write.format("orc").mode("append").saveAsTable(sTabellaCat)
					}
					else {
					    println("mode: "+mode +" not found") 
					}
			
	}
  
  def main(args: Array[String]) = {
    
    var retCode: Integer = -1
    try {
      System.setProperty("java.security.auth.login.config", "jaas-client_sdpbatch.conf");
      System.setProperty("spark.driver.allowMultipleContexts", "true");

      LOG.info("[[MetadataGovDashb_HiveCatalog::main]] BEGIN")
      
      var hiveMeta_url: String = ""
      var hiveMeta_username: String = ""
      var hiveMeta_schema: String = ""
      var hiveMeta_password: String = ""
      var timestamp: String = ""
      var queue:Option[String] = None
      ////Tabelle utilizzate dal notebook Cruscotto di Governo ////
      
      //tabellaCatalogo contiene numero record e occupazione spazio per tutte le tabelle Hive dei db di ingestion/stage/transf
      var tabellaCatalogo ="yucca_metadata.db_catalog_global"
      
      parser.parse(args, CliOptions()) match {
        case None =>
        // syntax error on command line
        case Some(opts) => {
          hiveMeta_url = opts.hiveMeta_url
          hiveMeta_username = opts.hiveMeta_username
          hiveMeta_password = opts.hiveMeta_password
          timestamp = opts.timestamp
          queue = Some(opts.queue)
        }
      }

      val valueQueue: String = queue match {
        case None => "produzione"
        case Some(s: String) => s
      }
      LOG.info("[[MetadataGovDashb_HiveCatalog::main]] hiveMeta_url --> " + hiveMeta_url)
      LOG.info("[[MetadataGovDashb_HiveCatalog::main]] hiveMeta_username --> " + hiveMeta_username)
      LOG.info("[[MetadataGovDashb_HiveCatalog::main]] timestamp --> " + timestamp)
      LOG.info("[[MetadataGovDashb_HiveCatalog::main]] queue --> " + valueQueue)
      
      val conf = new SparkConf().set("spark.yarn.queue", valueQueue).setExecutorEnv(Array(("java.security.auth.login.config", "jaas-client.conf")));
      val sparkContext = new SparkContext(conf)
      val sqlContextHive = new org.apache.spark.sql.hive.HiveContext(sparkContext)
      val ts = lit(timestamp).cast("timestamp")
      
      val strSqlDbs="(SELECT NAME FROM hivedb.DBS) a"
      val reader = sqlContextHive.read.format("jdbc").options(Map("driver"->"com.mysql.jdbc.Driver","url" -> hiveMeta_url,"user"->hiveMeta_username,"password"->hiveMeta_password))
      val df_hive_dbs=reader.option("dbtable", strSqlDbs).load

      //CONSIDERO solo i db Hive di ingestion e transformation
      val listadb=df_hive_dbs.filter("(NAME like ('db_%') or NAME like ('transf_%')) and (NAME not in ('db_csi_log','db_celi'))").map(r=>r(0).toString).toArray.toList

      var j=0
      var mode=""
      
      for (t <-0 to listadb.length-1) {
       val dbh=listadb(t)
       println(dbh)
       if (j==0) mode="overwrite" else mode="append"
       metaCatalog(dbh,mode,"%",sqlContextHive,tabellaCatalogo,hiveMeta_url,hiveMeta_username,hiveMeta_password)
       j=j+1
      }
      
            
    } catch {
      case e: Exception =>
        LOG.error("[[MetadataGovDashb_HiveCatalog::main]]" + e, e)
        throw e
    } finally {
      LOG.info("[[MetadataGovDashb_HiveCatalog::main]] END")

    }

    System.exit(retCode)

  }


}