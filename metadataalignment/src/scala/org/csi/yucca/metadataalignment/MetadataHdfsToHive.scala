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

object MetadataHdfsToHive {

  val LOG = LoggerFactory.getLogger(getClass)
  val zkHost: String = ""

  case class CliOptions(
      timestamp: String = "",
      queue: String = "") {
  }

  val parser = new scopt.OptionParser[CliOptions]("ingest") {
    head("Start ", "0.x")

    opt[String]("timestamp").required().action((timestamp, c) => { c.copy(timestamp = timestamp) })
      .text("Timestamp in form 2018-10-14 11:41:26 ")
    opt[String]("queue").optional().action((queue, c) => { c.copy(queue = queue) })
      .text("Queue (optional, default=produzione)")
  }
  
 
def main(args: Array[String]) = {
    
    import sys.process._
        
    var retCode: Integer = -1
    try {
      System.setProperty("java.security.auth.login.config", "jaas-client_sdpbatch.conf");
      System.setProperty("spark.driver.allowMultipleContexts", "true");

      LOG.info("[[MetadataHdfsToHive::main]] BEGIN")
      
      var timestamp: String = ""
      var queue:Option[String] = None
      //Scrive sulle tabelle
      //yucca_metadata.solr_tot_bytes --> contiene occupazione spazio in bytes e nome della collezione solr per ogni tenant
      //yucca_metadata.area_stage_bytes --> contiene occupazione spazio in bytes dell'area di stage per ogni tenant (db e files)
      
      parser.parse(args, CliOptions()) match {
        case None =>
        // syntax error on command line
        case Some(opts) => {
          timestamp = opts.timestamp
          queue = Some(opts.queue)
        }
      }
      val valueQueue: String = queue match {
        case None => "produzione"
        case Some(s: String) => s
      }
      LOG.info("[[MetadataHdfsToHive::main]] timestamp --> " + timestamp)

      val conf = new SparkConf().set("spark.yarn.queue",valueQueue).setExecutorEnv(Array(("java.security.auth.login.config", "jaas-client.conf")));
      val sparkContext = new SparkContext(conf)
      val sqlContextHive = new org.apache.spark.sql.hive.HiveContext(sparkContext)

      import sqlContextHive.implicits._
      
      val ts = lit(timestamp).cast("timestamp")
      
      //Crea tabella che contiene l'occupazione SOLR yucca_metadata.solr_tot_bytes
      //originariamente creava CSV -- yucca_metadata.collection_dim_ext:  hdfs dfs -du /user/solr | awk '{print $1","$2}' > 20180918_solr_dim.csv
      
      var sol_tot_bytes_today = sqlContextHive.emptyDataFrame
      var sol_tot_bytes_toappend = sqlContextHive.emptyDataFrame

      val h = "hdfs dfs -du /user/solr".!!
      
      val dfh=sparkContext.parallelize(h.split("\n").toList).toDF().selectExpr("regexp_replace(_1, '\\s+', ',') as str").selectExpr("substr(str,0,instr(str,',')-1) as byte","substr(str,instr(str,',')+1,length(str)) as collectionname")

      dfh.registerTempTable("tmp_collection_dim")
      var strSqlSolr=""
      strSqlSolr="select tenantcode, avg(byte) as solr_byte, solrcollectionname from yucca_metadata.meta_dataset_extended left join tmp_collection_dim on (upper(substring(collectionname,12)) == upper(solrcollectionname)) group by tenantcode, solrcollectionname"
      
      sol_tot_bytes_today= sqlContextHive.sql(strSqlSolr)
      sol_tot_bytes_today.write.mode("overwrite").format("orc").saveAsTable("yucca_metadata.solr_tot_bytes")
      
      sol_tot_bytes_toappend = sol_tot_bytes_today.withColumn("ts_inserimento", ts)
      sol_tot_bytes_toappend.write.mode("append").format("orc").saveAsTable("yucca_metadata.solr_tot_bytes_storico")


      //Tabella area_stage_bytes
      //originariamente hdfs dfs -du /datalake/*/dataset_transformed/STAGE/*/files | awk '{print $1","$2}'  #> 20180919_hdfs_stage_files.csv
      var dim_hive_stage_today = sqlContextHive.emptyDataFrame
      var dim_hive_stage_toappend = sqlContextHive.emptyDataFrame

      var h_s = "hdfs dfs -du /datalake/*/dataset_transformed/STAGE/*".!!
      
      var dfh_s=sparkContext.parallelize(h_s.split("\n").toList).toDF().selectExpr("regexp_replace(_1, '\\s+', ',') as strToParse")
      .selectExpr("substring(strToParse,0,instr(strToParse,',')-1) as byte","substring(strToParse,instr(strToParse,',')+1,length(strToParse)) as stg_path","substring(strToParse,instr(strToParse,'STAGE')+6) as tmpfield")
      .selectExpr("byte","stg_path","substring(tmpfield,0,instr(tmpfield,'/')-1) as tenantcode")
      
      dfh_s.registerTempTable("tmp_stage_dim")
      var strSqlStg=""
      strSqlStg="select * from tmp_stage_dim"
      
      dim_hive_stage_today= sqlContextHive.sql(strSqlStg)
      dim_hive_stage_today.write.mode("overwrite").format("orc").saveAsTable("yucca_metadata.area_stage_bytes")     
     
      dim_hive_stage_toappend = dim_hive_stage_today.withColumn("ts_inserimento", ts)
      dim_hive_stage_toappend.write.mode("append").format("orc").saveAsTable("yucca_metadata.area_stage_bytes_storico")
      
    } catch {
      case e: Exception =>
        LOG.error("[[MetadataHdfsToHive::main]]" + e, e)
        throw e
    } finally {
      LOG.info("[[MetadataHdfsToHive::main]] END")

    }

    System.exit(retCode)

  }


}