package org.csi.yucca.speedalignment.promotion
import org.slf4j.LoggerFactory
import collection.immutable
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.hive._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode

import org.csi.yucca.helper.AdminApiDelegate
import org.csi.yucca.dto.DatasetInfo
import org.csi.yucca.dto.TenantInfo
import org.csi.yucca.helper.HDFSHelper

import org.csi.yucca.adminapi.client.BackofficeListaClient
import org.joda.time.DateTime

import com.lucidworks.spark.util.ConfigurationConstants
import scopt._
import org.apache.spark.sql.DataFrame
object HivePromotionGruppiDs {
  val LOG = LoggerFactory.getLogger(getClass)

  case class CliOptions(
    groupId:             Integer  = -1,
    groupVersion:             Integer  = -1,
    prjName:             String  = "-",
    coordinatorName:     String  = "-",
    logStatisticsToHive: Boolean = false,
    addbdaInfo:          Boolean = false,
    addBdaUniqueId:      Boolean = false) {

  }

  val parser = new scopt.OptionParser[CliOptions]("ingest") {
    head("Simple ingestion cli", "0.x")

    opt[Boolean]("logStatisticsToHive")
      .action((code, c) => { c.copy(logStatisticsToHive = code) })
      .text("scrive log statistiche su hive db_csi_log")

    opt[String]("coordinatorName").action((code, c) => { c.copy(coordinatorName = code) })
      .text("nome coordinator")

    opt[Int]("groupId").required().action((code, c) => { c.copy(groupId = code) })
      .text("id del gruppo di dataset")
    opt[Int]("groupVersion").required().action((code, c) => { c.copy(groupVersion = code) })
      .text("versione del gruppo di dataset")

    opt[String]("prjName").action((code, c) => { c.copy(prjName = code) })
      .text("nome prl per log statistiche su hive")

    opt[Boolean]("addbdaInfo").action((code, c) => { c.copy(addbdaInfo = code) })
      .text("genera le colonne bda_id e bdaorigin")

    opt[Boolean]("addBdaUniqueId").action((code, c) => { c.copy(addBdaUniqueId = code) })
      .text("genera la colonna sparkuniqueid")

  }
  
  
  
  def main(args: Array[String]) = {
    var retCode: Integer = -1
    try {
      System.setProperty("java.security.auth.login.config", "jaas-client_sdpbatch.conf");
      System.setProperty("spark.driver.allowMultipleContexts", "true");

      LOG.info("[[HivePromotionGruppiDs::main]] BEGIN")

      var tenantCode: String = ""
      //var eleDsId: String = ""
      var toWrite: String = ""
      var forceCollectionTO: String = "none"

        var groupId:             Integer  = -1
    var groupVersion:             Integer  = -1
      
      

      var prjName: String = ""
      var logStatisticsToHive: Boolean = false
      var coordinatorName: String = ""
      var addbdaInfo: Boolean = false
      var addBdaUniqueId: Boolean = false

      parser.parse(args, CliOptions()) match {
        case None =>
        // syntax error on command line
        case Some(opts) => {
          groupId = opts.groupId
          groupVersion = opts.groupVersion
          prjName = opts.prjName
          logStatisticsToHive = opts.logStatisticsToHive
          coordinatorName = opts.coordinatorName
          addbdaInfo = opts.addbdaInfo
          addBdaUniqueId = opts.addBdaUniqueId
        }
      }

      val startFrom: Integer = 0

      val stepCnt: Integer = 100000

      LOG.info("[[HivePromotionGruppiDs::main]] prjName --> " + prjName)
      LOG.info("[[HivePromotionGruppiDs::main]] groupId --> " + groupId)
      LOG.info("[[HivePromotionGruppiDs::main]] groupVersion --> " + groupVersion)
      LOG.info("[[HivePromotionGruppiDs::main]] logStatisticsToHive --> " + logStatisticsToHive)
      LOG.info("[[HivePromotionGruppiDs::main]] coordinatorName --> " + coordinatorName)
      LOG.info("[[HivePromotionGruppiDs::main]] addbdaInfo --> " + addbdaInfo)
      LOG.info("[[HivePromotionGruppiDs::main]] addBdaUniqueId --> " + addBdaUniqueId)

      //TODO loggare parametri e descrivere configurazione e operazioni

      val conf = new SparkConf().set("spark.yarn.queue", "produzione").setExecutorEnv(Array(("java.security.auth.login.config", "jaas-client.conf")));
      val sparkContext = new SparkContext(conf)
      val sqlContextHive = new org.apache.spark.sql.hive.HiveContext(sparkContext)
      val adminDel = new AdminApiDelegate()

      
      
      
      
      
      val listads=adminDel.getDatasetsByGroup(sparkContext, groupId, groupVersion, true, false)
      
      var dfStats: DataFrame = null;
      retCode = 0;
      for (v <- listads) {
        var ds =v
        var dfCur = writeDataset(sqlContextHive, sparkContext, ds, stepCnt, startFrom, prjName, logStatisticsToHive, coordinatorName, addbdaInfo, addBdaUniqueId)
        LOG.info("[[HivePromotionGruppiDs::::main]] PROMOTION dataset " + ds.datasetCode + "(" + ds.idDataset + ") dfCur.size=" + dfCur.count())
        if (retCode == 0) dfStats = dfCur
        else dfStats = dfStats.unionAll(dfCur)
        retCode = retCode + 1
      }
      
      
      
      
      
      
      if (logStatisticsToHive == true && dfStats != null && dfStats.count() > 0) {
        dfStats.coalesce(1).write.format("orc").mode("append").saveAsTable("db_csi_log.yuccadatalake_promotion_logs")
      }

    } catch {
      case e: Exception =>
        LOG.error("[[HivePromotionGruppiDs::main]]", e)
        throw e
    } finally {
      LOG.info("[[HivePromotionGruppiDs::main]] END")

    }

    System.exit(retCode)

  }  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  def writeDataset(sqlContextHive: HiveContext, sparkContext: SparkContext,
                   ds:                  DatasetInfo,
                   stepCnt:             Integer,
                   startFrom:           Integer,
                   prjName:             String,
                   logStatisticsToHive: Boolean,
                   coordinatorName:     String,
                   addbdaInfo:          Boolean,
                   addBdaUniqueId:      Boolean) = {

    LOG.info("[[HivePromotionGruppiDs::main]] srcHiveTable = " + ds.jdbcdbhive + "." + ds.jdbctablename)
    LOG.info("[[HivePromotionGruppiDs::main]] targetHiveTable = " + ds.dbHiveSchema + "." + ds.dbHiveTable)

    //DROP TABLE
    var dropTable = sqlContextHive.sql("DROP TABLE " + ds.dbHiveSchema + "." + ds.dbHiveTable)

    var objectId = sqlContextHive.sql("create temporary function getObjectId as 'it.csi.yucca.hive.UDFObjectId' using jar 'hdfs:///csi_lib/hive/hive-ext-objectid-1.0.0-001.jar'");

    //CREATE TABLE
    
    LOG.info("[[HivePromotionGruppiDs::main]] targetHiveTable = " + ds.dbHiveSchema + "." + ds.dbHiveTable + " cols : "+getColumns(ds))
    
    //var strSqlCreate = "create table " + ds.dbHiveSchema + "." + ds.dbHiveTable + " stored as orc as select * "
    var strSqlCreate = "create table " + ds.dbHiveSchema + "." + ds.dbHiveTable + " stored as orc as select  "+getColumns(ds)

    if (addbdaInfo == true) strSqlCreate = strSqlCreate + ",getObjectId() as bda_id,'datalake' as bda_origin "

    if (addBdaUniqueId == true) strSqlCreate = strSqlCreate + ",row_number() OVER () as sparkuniqueid "

    strSqlCreate = strSqlCreate + "from " + ds.jdbcdbhive + "." + ds.jdbctablename

    //Timestamp iniziale per scrittura log su Hive
    val start = System.currentTimeMillis
    LOG.info("[[HivePromotionGruppiDs::main]] doing--> "+strSqlCreate)

    var hiveTable = sqlContextHive.sql(strSqlCreate)

    //Tempo trascorso per scrittura dati
    val elapsedTime = System.currentTimeMillis - start

    
   val numrecord = sqlContextHive.sql("select * from " + ds.dbHiveSchema + "." + ds.dbHiveTable).count();
     
   LOG.info("[[HivePromotionGruppiDs::main]] numrecord = " + numrecord)

    var logRow = sqlContextHive.createDataFrame(Seq((DateTime.now().toString(),
      sparkContext.applicationId,
      sparkContext.appName,
      prjName,
      ds.jdbcdbhive + "." + ds.jdbctablename,
      ds.dbHiveSchema + "." + ds.dbHiveTable,
      numrecord,
      elapsedTime,
      ds.datasetCode,
      coordinatorName))).toDF("timeoperation", "id_application_yarn", "spark_app_name", "prjname", "srchivetable", "targethivetable", "numrec", "elapsed_type", "dataset_code", "coordinator_name")

    logRow
  }
  
def getColumns  (ds:DatasetInfo) : String = {
    
      val campiFiltered1 = ds.campi.filterNot(_.fieldName.toLowerCase().equals("bda_id") )
      val campiFiltered2 = campiFiltered1.filterNot(_.fieldName.toLowerCase().equals("bda_origin") )
      val campiFiltered = campiFiltered2.filterNot(_.fieldName.toLowerCase().equals("sparkuniqueid") )
    
			val strColsTmp=campiFiltered.map( field => 
			"" + field.fieldName.toLowerCase            // keep the name around "safety" quotes :) 
		//	+ "  "
	//		+ field.dataType + "("+field.dtype+")"
			
					).mkString(", ").toLowerCase
			
			
			
			return strColsTmp
  }       
}