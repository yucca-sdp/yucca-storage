package org.csi.yucca.speedalignment.downloadCsv


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

import org.csi.yucca.helper.SolrDelegate
import org.csi.yucca.helper.AdminApiDelegate
import org.csi.yucca.dto.DatasetInfo
import org.csi.yucca.dto.TenantInfo
import org.csi.yucca.helper.HDFSHelper

import org.csi.yucca.adminapi.client.BackofficeListaClient
import org.joda.time.DateTime




import com.lucidworks.spark.util.ConfigurationConstants
import scopt._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.DataFrame
import org.apache.commons.csv.QuoteMode
import org.bson.types.ObjectId
import org.apache.commons.lang.time.DateUtils
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.permission.FsPermission

object HiveToCsvMultiDataset {
  
  
  	val LOG = LoggerFactory.getLogger(getClass) 
		val dataTypes = immutable.Map(                

				"boolean"        -> Seq( "_b",  "BOOLEAN"   ),
				"string"         -> Seq( "_s",  "VARCHAR"   ),
				"int"            -> Seq( "_i",  "INTEGER"   ),
				"long"           -> Seq( "_l",  "BIGINT"    ),
				"double"         -> Seq( "_d",  "DOUBLE"    ),
				"data"           -> Seq( "_dt", "TIMESTAMP" ),
				"date"           -> Seq( "_dt", "TIMESTAMP" ),
				"datetimeOffset" -> Seq( "_dt", "TIMESTAMP" ),
				"datetime"       -> Seq( "_dt", "TIMESTAMP" ),
				"dateTime"       -> Seq( "_dt", "TIMESTAMP" ),
				"time"           -> Seq( "_dt", "TIMESTAMP" ),
				"float"          -> Seq( "_f",  "FLOAT"     ),
				"longitude"      -> Seq( "_d",  "DOUBLE"    ),
				"latitude"       -> Seq( "_d",  "DOUBLE"    ),
				"binary"         -> Seq( "_s",  "VARCHAR"   ),
				"bigdecimal"     -> Seq( "_d",  "DOUBLE"    )

				);
  val WRITEMODE_NOWRITE:String="nowrite"	
  val WRITEMODE_APPEND:String="append"	
  val WRITEMODE_OVERWRITE:String="overwrite"	
  
   val ADMIN_API_URL = "<place-holder-to-admin-http-base-uri>"
  
  
case class CliOptions (
    tenantCode: String = "",
    eleDsId: String = "",
    toWrite: String = "",
    prjName: String = "-",
    coordinatorName: String = "-",
    logStatisticsToHive : Boolean = false
) {
 

}
 
  val parser = new scopt.OptionParser[CliOptions]("ingest") {
    head("Simple ingestion cli", "0.x")

    
    opt[Boolean]("logStatisticsToHive")
      .action( (code,c) => {c.copy(logStatisticsToHive = code)} )
      .text("scrive log statistiche su hive db_csi_log")

    opt[String]("tenantCode").required().action( (code,c) => { c.copy(tenantCode = code)})
      .text("codice tenant")
    opt[String]("coordinatorName").action( (code,c) => { c.copy(coordinatorName = code)})
      .text("nome coordinator")

    opt[String]("toWrite").required().action( (code,c) => { c.copy(toWrite = code)})
      .text("modalità di scrittura")
    opt[String]("prjName").action( (code,c) => { c.copy(prjName = code)})
      .text("nome prl per log statistiche su hive")
 
    opt[String]("eleDsId").required().action( (code,c) => { c.copy(eleDsId = code)})
      .text("elenco id dataset da pubblicare")
      
  }
  
  
  
	def main(args:Array[String]) = {
    var retCode:Integer = -1
  try {
    	System.setProperty("java.security.auth.login.config", "jaas-client_sdpbatch.conf");
			System.setProperty("spark.driver.allowMultipleContexts", "true");
			System.setProperty("solr.jaas.conf.path","jaas-client.conf");

    
		LOG.info("[[HiveToCsvMultiDataset::main]] BEGIN")

		
			var tenantCode: String =""
			var eleDsId: String = ""
			var toWrite: String = ""
			var forceCollectionTO: String ="none"
			
			var prjName:String = "" 
			var logStatisticsToHive:Boolean=false      
			     var coordinatorName:String =""
			     
			     
    parser.parse( args, CliOptions()) match {
      case None =>
        // syntax error on command line
      case Some(opts) => {
        tenantCode=opts.tenantCode
        eleDsId=opts.eleDsId
        toWrite=opts.toWrite
        prjName=opts.prjName
        logStatisticsToHive=opts.logStatisticsToHive
        coordinatorName=opts.coordinatorName
        
      }
    }

		  LOG.info("[[HiveToCsvMultiDataset::main]] tenantCode --> "+tenantCode)
		  LOG.info("[[HiveToCsvMultiDataset::main]] idDataset --> "+eleDsId)
		  LOG.info("[[HiveToCsvMultiDataset::main]] toWrite --> "+toWrite)
		  LOG.info("[[HiveToCsvMultiDataset::main]] logStatisticsToHive --> "+logStatisticsToHive)
		  LOG.info("[[HiveToCsvMultiDataset::main]] prjName --> "+prjName)
		  LOG.info("[[HiveToCsvMultiDataset::main]] coordinatorName --> "+coordinatorName)
			
		  if (! (WRITEMODE_NOWRITE.equalsIgnoreCase(toWrite)  ||  WRITEMODE_APPEND.equalsIgnoreCase(toWrite) || WRITEMODE_OVERWRITE.equalsIgnoreCase(toWrite)  ) ) {
		    throw new Exception("invalid write mode '"+toWrite+"' !! valid values are nowrite, append, overwrite ") 
		  }

			val conf = new SparkConf().set("spark.yarn.queue","produzione").setExecutorEnv(Array(("java.security.auth.login.config", "jaas-client.conf")));
			val sparkContext = new SparkContext(conf)
			sparkContext.hadoopConfiguration.set("mapreduce.output.fileoutputformat.compress", "false")
			val sqlContextHive=new org.apache.spark.sql.hive.HiveContext(sparkContext)

			val adminDel=new AdminApiDelegate()

			val arrDsId = eleDsId.split(',')
			var dfStats:DataFrame = null;
			retCode=0;
			for (v <- arrDsId) {
						var idDataset:Integer =Integer.parseInt(v)

			      var ds=adminDel.getDatasets4PromotionByIdDataset(sparkContext, idDataset)
			      var dfCur=writeCsv(sqlContextHive,sparkContext,ds,toWrite,prjName,logStatisticsToHive,coordinatorName )
            adminDel.createOrUpdateAllineamento(ds.idDataset, ds.datasetVersion, "f48648f00000000000000000",ds.idOrganization)
			      
			      LOG.info("[[HiveToCsvMultiDataset::::main]] Write dataset "+ ds.datasetCode +"("+ds.idDataset+") dfCur.size="+dfCur.count())
			
			      if (retCode==0) dfStats=dfCur
			      else dfStats=dfStats.unionAll(dfCur)
			      retCode=retCode+1
			      
      }
			if (logStatisticsToHive==true && dfStats!=null && dfStats.count()>0) {
			  dfStats.write.format("orc").mode("append").saveAsTable("db_csi_log.yuccadatalake_downloadcsv_logs ")
			}
  
		
		
		
	  } catch  {
	    case e: Exception => LOG.error("[[HiveToCsvMultiDataset::main]]", e)
	     throw e
	  } finally {
			LOG.info("[[HiveToCsvMultiDataset::main]] END")
	    
	  }
	  
    System.exit(retCode)
	  
	}
  
  
  def writeCsv(sqlContextHive:HiveContext,sparkContext:SparkContext,
      ds:DatasetInfo,
      toWrite:String,
      prjName:String,logStatisticsToHive : Boolean,coordinatorName:String) : DataFrame = {
    
      val dsType = ds.subType match { 
					case "bulkDataset"   => "data"
					case "socialDataset" => "social"
					case _ => "measures"
			}				

						
			var strCols=getSolrColumns(ds,dsType)
			
			LOG.info("[[HiveToCsvMultiDataset::main]]strCols = "+strCols)
			LOG.info("[[HiveToCsvMultiDataset::main]]availableSpeed = "+ds.availableSpeed)
			LOG.info("[[HiveToCsvMultiDataset::main]]hive = "+ds.dbHiveSchema+"."+ds.dbHiveTable)
			

		
      val writeMode:SaveMode = toWrite.toLowerCase() match { 
					case "overwrite"   => SaveMode.Overwrite
					case "append" => SaveMode.Append
					case _ => SaveMode.Overwrite
			}		

      
      var hiveTable=sqlContextHive.sql(" select sparkuniqueid,"+strCols+" from "+ds.dbHiveSchema+"."+ds.dbHiveTable )
      
      
       hiveTable = hiveTable.select(
          hiveTable.columns.
        map(c => (
          if (c.endsWith("_dt"))
            date_format(to_utc_timestamp(col(c), java.util.TimeZone.getDefault.getID), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias(c)
          else if (c.endsWith("_s"))
            coalesce(col(c), lit("")).alias(c)
          else if (c.endsWith("_f"))
            col(c).cast(FloatType).cast(StringType).alias(c)
          else col(c))): _*)

      // Here we "drop" (aka remove...) columns we don't want in our CSV ...
      hiveTable = hiveTable.drop("id")
      hiveTable = hiveTable.drop("iddataset_l")
      hiveTable = hiveTable.drop("datasetversion_l")
      hiveTable = hiveTable.drop("origin_s")

      hiveTable = hiveTable.columns.foldLeft(hiveTable)((_hiveTable, _column) =>
      _hiveTable.withColumnRenamed(_column.toString, _column.toString.toLowerCase.replaceFirst("(_[^_]+$)", "")))
        
        
			val numrecordTowrite = hiveTable.count()      
			LOG.info("[[HiveToCsvMultiDataset::main]]   Numero Record hive da scrivere  --> "+numrecordTowrite)
      
			var hdfsHelper= new HDFSHelper(null)
   
			//CANCELLAZIONE IN CASO DI REPLACE
			if ( WRITEMODE_OVERWRITE.equalsIgnoreCase(toWrite)) {
			  LOG.info("[[HiveToCsvMultiDataset::main]]   deleting from hdfs --> tenant:" +ds.organizationCode + "   domain: "+ds.dataDomain  + "   subdomain: "+ ds.codSubDomain + " dssubtype:" +ds.subType + " dscode:" +ds.datasetCode+ " streamcode:" + ds.streamCode + " slug:" + ds.vESlug)
    		if (!(WRITEMODE_NOWRITE.equalsIgnoreCase(toWrite))) hdfsHelper.deleteDataSetForCsv(sparkContext.hadoopConfiguration, ds.organizationCode, ds.dataDomain, ds.codSubDomain, ds.subType, ds.datasetCode, ds.streamCode, ds.vESlug)
				LOG.info("[[HiveToCsvMultiDataset::main]] deleting")
				Thread.sleep(10000)
			}
	
			

		
			val colsToSelect = hiveTable.columns.filter(_ != "sparkuniqueid")
 
			
      val newObjectId = createObjectId() 
	 			
			//Timestamp iniziale per scrittura log su Hive	
			val start = System.currentTimeMillis		
			var dfTowrite=hiveTable.select(colsToSelect.head, colsToSelect.tail: _*)
			
			LOG.info("[[HiveToCsvMultiDataset::DATAFRAMEE]]" + dfTowrite.show())
			dfTowrite.printSchema()
		  
			var __csvExportPath = "tmp/scarico/" + newObjectId + "/" + ds.organizationCode.toUpperCase() + "/" + "_" + f"${ds.idDataset}%05d-${ds.datasetVersion}%03d.csv.tmp"
      var __finalfilename = newObjectId + "_" + numrecordTowrite + "-" + ds.datasetCode + "-" + ds.datasetVersion + ".csv"

      saveDataFrame(dfTowrite, __csvExportPath)

      var finalDir = hdfsHelper.getFinalDir(ds.subType, ds.organizationCode, ds.dataDomain, ds.codSubDomain, ds.vESlug)
      var lastDir = hdfsHelper.getLastDir(ds.subType, ds.datasetCode, ds.streamCode)
      val fileSystem = FileSystem.get(sparkContext.hadoopConfiguration)

      hdfsHelper.mkdirs(fileSystem, finalDir, lastDir)

      var fileName = fileSystem.globStatus(new Path(__csvExportPath + "/part*"))(0).getPath.getName

      println("--filename:" + __csvExportPath + "/" + fileName)
      println("--new:" + finalDir + "/" + lastDir + "/" + __finalfilename)

      fileSystem.rename(new Path(__csvExportPath + "/" + fileName), new Path(finalDir + "/" + lastDir + "/" + __finalfilename))

      dfTowrite.unpersist()

  
			
			//Tempo trascorso per scrittura dati su CSV
			val elapsedTime = System.currentTimeMillis - start

			
			//if (true==logStatisticsToHive ) {
		
        var logRow = sqlContextHive.createDataFrame(Seq((DateTime.now().toString(), 
	        sparkContext.applicationId, 
	        sparkContext.appName, 
	        prjName,
	        ds.dbHiveSchema+"."+ds.dbHiveTable, 
	        numrecordTowrite, 
	        elapsedTime,
	        ds.datasetCode,coordinatorName,finalDir + "/" + lastDir + "/" + __finalfilename,"datalake",-1,-1))).toDF("timeoperation","id_application_yarn","spark_app_name","prjname","hivetable","record_read","elapsed_tyme","dataset_code","coordinator_name","hdfs_path","download_type" ,"min_id" ,"max_id" )

			return logRow
  }
  
  
  def getSolrColumns  (ds:DatasetInfo, dsType:String) : String = {
    
      val campiFiltered1 = ds.campi.filterNot(_.fieldName.toLowerCase().equals("bda_id") )
      val campiFiltered2 = campiFiltered1.filterNot(_.fieldName.toLowerCase().equals("bda_origin") )
      val campiFiltered = campiFiltered2.filterNot(_.fieldName.toLowerCase().equals("sparkuniqueid") )
    
			val strColsTmp=campiFiltered.map( field =>
			"" + field.fieldName.toLowerCase            // keep the name around "safety" quotes :)
			+ " as "
			+ field.fieldName.toLowerCase            // keep the name around "safety" quotes :)
			+ dataTypes.get(field.dataType).get(0).toLowerCase
					).mkString(", ")
			
     var strColsstream = dsType match {
  			case "measures" => "bda_time as time_dt, bda_sensor as sensor_s,  bda_streamcode as streamcode_s,"
  			case "social"    => "bda_time as time_dt, bda_sensor as sensor_s,  bda_streamcode as streamcode_s,"
  			case _ => ""
			}
			
			
			//if (ds.idDataset==3268) strColsstream="data_prestazione as time_dt, 'cebe1641-58eb-411c-d0fb-f81e07a1dcef' as sensor_s, 'TempiAttesa_1415' as streamcode_s,"
			
    	var strCols="bda_id as id, " +ds.idDataset +" as iddataset_l, " + ds.datasetVersion +" as datasetversion_l, bda_origin as origin_s, "
			strCols+=strColsstream
			strCols+=strColsTmp
			return strCols
  }    
  
  def saveDataFrame(df: DataFrame, __csvExportPath: String) = {

    LOG.info("SparkDatasetsDownloader.saveDataFrame() ==> BEGIN")

    df.coalesce(1).write.format("com.databricks.spark.csv").
      option("header", "true").
      option("quoteMode", QuoteMode.MINIMAL.toString).
      option("treatEmptyValuesAsNulls", "false").
      mode("overwrite").
      save(__csvExportPath)
      
    LOG.info("ThreadDatasetDownloadCSV.saveDataFrame() ==> END")
  }
  
  def createObjectId() = {
    new ObjectId(DateUtils.addMinutes(new java.util.Date, -7)).toString()
  }
}
  
  
  

