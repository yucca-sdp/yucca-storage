package org.csi.yucca.speedalignment.multi


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

object HiveToSolrMultiDataset {
  
  
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
  
  /*def getDataset(adminApiUrl: String, tenantCode: String) = {
    val datasets = BackofficeListaClient.getListStreamDatasetByTenantCode(adminApiUrl, tenantCode, HiveToSolrMultiDataset.toString())
    LOG.info("Trovati " + datasets.size() + " datasets.")
    datasets
  }*/
  
  
  
  val zkHost: String = ""	 
  
case class CliOptions (
    tenantCode: String = "",
    eleDsId: String = "",
    toWrite: String = "",
    forceCollectionTO: String = "none",
    prjName: String = "-",
    coordinatorName: String = "-",
    logStatisticsToHive : Boolean = false,
    alignDate : Boolean = false
) {
 

}
 
  val parser = new scopt.OptionParser[CliOptions]("ingest") {
    head("Simple ingestion cli", "0.x")

    
    opt[Boolean]("alignDate")
      .action( (code,c) => {c.copy(alignDate = code)} )
       .text("crea o aggiorna record su tabella di allineamento")
      
     opt[Boolean]("logStatisticsToHive")
      .action( (code,c) => {c.copy(logStatisticsToHive = code)} )
      .text("scrive log statistiche su hive db_csi_log")

    opt[String]("tenantCode").required().action( (code,c) => { c.copy(tenantCode = code)})
      .text("codice tenant")
    opt[String]("coordinatorName").action( (code,c) => { c.copy(coordinatorName = code)})
      .text("nome coordinator")

    opt[String]("toWrite").required().action( (code,c) => { c.copy(toWrite = code)})
      .text("modalità di scrittura")
    opt[String]("forceCollectionTO").action( (code,c) => { c.copy(forceCollectionTO = code)})
      .text("forza collection solr di destinazione a .... ")
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

    
		LOG.info("[[HiveToSolrMultiDataset::main]] BEGIN")

		
			var tenantCode: String =""
			var eleDsId: String = ""
			var toWrite: String = ""
			var forceCollectionTO: String ="none"
			
			var prjName:String = "" 
			var logStatisticsToHive:Boolean=false      
			var coordinatorName:String =""
			var alignDate:Boolean=false
			     
			     
    parser.parse( args, CliOptions()) match {
      case None =>
        // syntax error on command line
      case Some(opts) => {
        tenantCode=opts.tenantCode
        eleDsId=opts.eleDsId
        toWrite=opts.toWrite
        forceCollectionTO=opts.forceCollectionTO
        prjName=opts.prjName
        logStatisticsToHive=opts.logStatisticsToHive
        coordinatorName=opts.coordinatorName
        alignDate=opts.alignDate
        
      }
    }
			     
			     
			     
			     
			val startFrom:Integer =0
			
			val stepCnt: Integer = 100000
			

		  LOG.info("[[HiveToSolrMultiDataset::main]] tenantCode --> "+tenantCode)
		  LOG.info("[[HiveToSolrMultiDataset::main]] idDataset --> "+eleDsId)
		  LOG.info("[[HiveToSolrMultiDataset::main]] toWrite --> "+toWrite)
		  LOG.info("[[HiveToSolrMultiDataset::main]] forceCollectionTO --> "+forceCollectionTO)
		  LOG.info("[[HiveToSolrMultiDataset::main]] logStatisticsToHive --> "+logStatisticsToHive)
		  LOG.info("[[HiveToSolrMultiDataset::main]] prjName --> "+prjName)
		  LOG.info("[[HiveToSolrMultiDataset::main]] coordinatorName --> "+coordinatorName)
		  LOG.info("[[HiveToSolrMultiDataset::main]] alignDate --> "+alignDate)
			
		  if (! (WRITEMODE_NOWRITE.equalsIgnoreCase(toWrite)  ||  WRITEMODE_APPEND.equalsIgnoreCase(toWrite) || WRITEMODE_OVERWRITE.equalsIgnoreCase(toWrite)  ) ) {
		    throw new Exception("invalid write mode '"+toWrite+"' !! valid values are nowrite, append, overwrite ") 
		  }
		  
			
			//TODO loggare parametri e descrivere configurazione e operazioni
			
			
			val conf = new SparkConf().set("spark.yarn.queue","produzione").setExecutorEnv(Array(("java.security.auth.login.config", "jaas-client.conf")));
			val sparkContext = new SparkContext(conf)
			val sqlContextHive=new org.apache.spark.sql.hive.HiveContext(sparkContext)

			val solrDelegate = new SolrDelegate(zkHost);
			val adminDel=new AdminApiDelegate()

			val arrDsId = eleDsId.split(',')
			var dfStats:DataFrame = null;
			retCode=0;
			for (v <- arrDsId) {
						var idDataset:Integer =Integer.parseInt(v)

			      var ds=adminDel.getDatasets4PromotionByIdDataset(sparkContext, idDataset)
			      var dfCur=writeDataset(solrDelegate,sqlContextHive,sparkContext,ds,stepCnt,toWrite,forceCollectionTO,startFrom,prjName,logStatisticsToHive,coordinatorName )
			      if (alignDate==true)
			        adminDel.createOrUpdateAllineamento(ds.idDataset, ds.datasetVersion, "f48648f00000000000000000",ds.idOrganization)
			      LOG.info("[[HiveToSolrMultiDataset::::main]] PUHLISHED dataset "+ ds.datasetCode +"("+ds.idDataset+") dfCur.size="+dfCur.count())
			      //if (retCode<0) throw new Exception( " PUHLISHED  FAIL dataset "+ ds.datasetCode +"("+ds.idDataset+") retcode="+retCode)
			      if (retCode==0) dfStats=dfCur
			      else dfStats=dfStats.unionAll(dfCur)
			      retCode=retCode+1
			      
      }
			if (logStatisticsToHive==true && dfStats!=null && dfStats.count()>0) {
			  dfStats.write.format("orc").mode("append").saveAsTable("db_csi_log.yuccadatalake_publish_logs")
			}
  
		
		
		
	  } catch  {
	    case e: Exception => LOG.error("[[HiveToSolrMultiDataset::main]]", e)
	     throw e
	  } finally {
			LOG.info("[[HiveToSolrMultiDataset::main]] END")
	    
	  }
	  
    System.exit(retCode)
	  
	}
  
  
  def writeDataset(solrDelegate:SolrDelegate,sqlContextHive:HiveContext,sparkContext:SparkContext,
      ds:DatasetInfo,
      stepCnt:Integer,
      toWrite:String,
      forceCollectionTO: String,
      startFrom:Integer,
      prjName:String,logStatisticsToHive : Boolean,coordinatorName:String) : DataFrame = {
    
      val dsType = ds.subType match { 
					case "bulkDataset"   => "data"
					case "socialDataset" => "social"
					case _ => "measures"
			}				

      //Valorizzo SolrCollection
			var solrCollection = ds.solrCollection
			if ( !("none".equalsIgnoreCase(forceCollectionTO)) ) {
			  solrCollection=forceCollectionTO
			}			
						
			var strCols=getSolrColumns(ds,dsType)
			
			LOG.info("[[HiveToSolrMultiDataset::main]]strCols = "+strCols)
			LOG.info("[[HiveToSolrMultiDataset::main]]solrCollection = "+solrCollection)
			LOG.info("[[HiveToSolrMultiDataset::main]]availableSpeed = "+ds.availableSpeed)
			LOG.info("[[HiveToSolrMultiDataset::main]]hive = "+ds.dbHiveSchema+"."+ds.dbHiveTable)
			

			//numero record iniziali su solr
      val countInitSolr = solrDelegate.countDocument(ds.idDataset, -1, solrCollection, null, null)
      val writeMode:SaveMode = toWrite.toLowerCase() match { 
					case "overwrite"   => SaveMode.Overwrite
					case "append" => SaveMode.Append
					case _ => SaveMode.Overwrite
			}		
			LOG.info("[[HiveToSolrMultiDataset::main]]   Conteggio iniziale record Solr --> "+countInitSolr)

      
      var hiveTable=sqlContextHive.sql(" select sparkuniqueid,"+strCols+" from "+ds.dbHiveSchema+"."+ds.dbHiveTable )
					
					//hiveTable.printSchema();
      hiveTable = hiveTable.select(
          hiveTable.columns.
              map(c => (
                  if (c.endsWith("_dt")) col(c).cast(DateType)
                  else if (c.endsWith("_i")) col(c).cast(IntegerType)
                  else if (c.endsWith("_l")) col(c).cast(LongType)
                  else if (c.endsWith("_b")) col(c).cast(BooleanType)
                  else if (c.endsWith("_d")) col(c).cast(DoubleType)
                  else if (c.endsWith("_f")) col(c).cast(FloatType)
                  else if (c.endsWith("_s")) col(c).cast(StringType)
                  
                  else col(c))
           ): _*
                  
      )

        
        
			val numrecordTowrite = hiveTable.count()      
			LOG.info("[[HiveToSolrMultiDataset::main]]   Numero Record hive da scrivere  --> "+numrecordTowrite)
      
			
			//CANCELLAZIONE IN CASO DI REPLACE
      var countAfterDele = countInitSolr
			if ( WRITEMODE_OVERWRITE.equalsIgnoreCase(toWrite)) {
			  LOG.info("[[HiveToSolrMultiDataset::main]]   deleting from hdfs --> tenant:" +ds.organizationCode + "   domain: "+ds.dataDomain  + "   subdomain: "+ ds.codSubDomain + " dssubtype:" +ds.subType + " dscode:" +ds.datasetCode+ " streamcode:" + ds.streamCode + " slug:" + ds.vESlug)
  			var hdfsHelper= new HDFSHelper(null)
			  //eliminata la cancellazione dei csv perhè ora viene eseguita nello scarico del csv
    		//if (!(WRITEMODE_NOWRITE.equalsIgnoreCase(toWrite))) hdfsHelper.deleteDataSet4promotion(sparkContext.hadoopConfiguration, ds.organizationCode, ds.dataDomain, ds.codSubDomain, ds.subType, ds.datasetCode, ds.streamCode, ds.vESlug)
				LOG.info("[[HiveToSolrMultiDataset::main]] deleting")
				if (!(WRITEMODE_NOWRITE.equalsIgnoreCase(toWrite))) solrDelegate.deleteDocsForPromotion(ds.idDataset, ds.datasetVersion, solrCollection)
				Thread.sleep(10000)
        countAfterDele = solrDelegate.countDocument(ds.idDataset, -1, solrCollection, null, null)
				LOG.info("[[HiveToSolrMultiDataset::main]] Conteggio record Solr dopo cancellazione cnt -->" +countAfterDele)
			}
			
			//var counter=0
			
			var counter=startFrom
			
			var cicli=0
			val colsToSelect = hiveTable.columns.filter(_ != "sparkuniqueid")
    	var solroptions = Map(
    			"zkhost" -> zkHost,
    			"collection" -> solrCollection,
    			"gen_uniq_key" -> "true",
    			"fields" -> "iddataset_l,datasetversion_l",
    			"solr.params" -> "fl=iddataset_l,datasetversion_l"
    			)
			
      solroptions = Map(
    			ConfigurationConstants.SOLR_ZK_HOST_PARAM -> zkHost,
    			ConfigurationConstants.SOLR_COLLECTION_PARAM -> solrCollection,
    			ConfigurationConstants.GENERATE_UNIQUE_KEY -> "true",
    			ConfigurationConstants.SOLR_FIELD_PARAM -> "iddataset_l,datasetversion_l",
    			ConfigurationConstants.ARBITRARY_PARAMS_STRING -> "fl=iddataset_l,datasetversion_l"
    			)
    			
    			
			//Timestamp iniziale per scrittura log su Hive	
			val start = System.currentTimeMillis	
					
			LOG.info("[[HiveToSolrMultiDataset::main]] PHASE WRITING - START "+colsToSelect)
			do {
				var toRow=counter+stepCnt
						LOG.info("[[HiveToSolrMultiDataset::main]]      write from "+counter+" to "+ toRow +" of " + numrecordTowrite)

						var dfTowrite=hiveTable.filter(col("sparkuniqueid") >= lit(counter) ).filter(hiveTable.col("sparkuniqueid") <= lit(toRow) ).select(colsToSelect.head, colsToSelect.tail: _*)


						if (!(WRITEMODE_NOWRITE.equalsIgnoreCase(toWrite))) {
						  //dfTowrite.write.format("solr").options(solroptions).mode(writeMode).save
						  dfTowrite.write.format("org.csi.yucca.storageutils.helper.solrformat").options(solroptions).mode(writeMode).save
						  
						  
						  LOG.info("[[HiveToSolrMultiDataset::main]]      block done "+dfTowrite.count())
						} else {
						  LOG.info("[[HiveToSolrMultiDataset::main]]      block skipped - nowrite ")
						  
						}
						Thread.sleep(10000) 
						LOG.info("[[HiveToSolrMultiDataset::main]]      sleep done")

						counter=counter+stepCnt
						cicli=cicli+1 
			} while (counter < numrecordTowrite && cicli <150)
			LOG.info("[[HiveToSolrMultiDataset::main]] PHASE WRITING - END ")
			
			//Tempo trascorso per scrittura dati su Solr
			val elapsedTime = System.currentTimeMillis - start
			
      var countAfterWriting = solrDelegate.countDocument(ds.idDataset, ds.datasetVersion, solrCollection, null, null)
			var recWritten=countAfterWriting-countAfterDele
			LOG.info("[[HiveToSolrMultiDataset::main]] Conteggio record Solr dopo scrittura cnt -->" +countAfterWriting)
			LOG.info("[[HiveToSolrMultiDataset::main]] WRITE DONE "+ds.datasetCode +"("+ds.idDataset+ ") Scritti  "+recWritten+" di "+numrecordTowrite)
      
			
			//if (true==logStatisticsToHive ) {
		
      var logRow = sqlContextHive.createDataFrame(Seq((DateTime.now().toString(), 
	        sparkContext.applicationId, 
	        sparkContext.appName, 
	        prjName,
	        "multi",
	        ds.dbHiveSchema+"."+ds.dbHiveTable, 
	        ds.solrCollection, 
	        numrecordTowrite, 
	        recWritten,
	        elapsedTime,
	        ds.datasetCode,coordinatorName))).toDF("timeoperation","id_application_yarn","spark_app_name","prjname","publish_type","hivetable","solr_destination","record_read","record_write","elapsed_type","dataset_code","coordinator_name")
    
	    //logRow.write.format("orc").mode("append").saveAsTable("db_csi_log.yuccadatalake_publish_logs")
			//}
      if (recWritten!=numrecordTowrite)	{
  			LOG.warn("[[HiveToSolrMultiDataset::main]] quadratura errata delta" + (numrecordTowrite-recWritten))
  			//return -1
  			throw new Exception( " PUHLISHED  FAIL dataset "+ ds.datasetCode +"("+ds.idDataset+") ") 
      } 
			return logRow
  }
  
  def getSolrCollection (ds:DatasetInfo, tenant:TenantInfo, defCollection:String , dsType:String) : String = {
			
			
			var solrCollection = dsType match {
  			case "measures" => tenant.solrCollection_MEASURES
  			case "data"     => tenant.solrCollection_DATA
  			case "social"   => tenant.solrCollection_SOCIAL
  			case "media"    => tenant.solrCollection_MEDIA       // not used (.)
			}
			
			
			if ( !("none".equalsIgnoreCase(defCollection)) ) {
			  solrCollection=defCollection
			}
			
			return solrCollection
			
			
    
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
  
  
  
}

