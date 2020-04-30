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
object HiveToSolrGruppiDs {
  
  
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
    val datasets = BackofficeListaClient.getListStreamDatasetByTenantCode(adminApiUrl, tenantCode, HiveToSolrGruppiDs.toString())
    LOG.info("Trovati " + datasets.size() + " datasets.")
    datasets
  }*/
  
  
  
  val zkHost: String = ""	 
  
case class CliOptions (
    groupId:             Integer  = -1,
    groupVersion:             Integer  = -1,
    tenantCode: String = "",
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
    opt[Int]("groupId").required().action((code, c) => { c.copy(groupId = code) })
      .text("id del gruppo di dataset")
    opt[Int]("groupVersion").required().action((code, c) => { c.copy(groupVersion = code) })
      .text("versione del gruppo di dataset")
 
      
  }
  
  
  
	def main(args:Array[String]) = {
    var retCode:Integer = -1
  try {
    	System.setProperty("java.security.auth.login.config", "jaas-client_sdpbatch.conf");
			System.setProperty("spark.driver.allowMultipleContexts", "true");
			System.setProperty("solr.jaas.conf.path","jaas-client.conf");

    
		LOG.info("[[HiveToSolrGruppiDs::main]] BEGIN")

		
			var tenantCode: String =""
			var toWrite: String = ""
			var forceCollectionTO: String ="none"
			
			var prjName:String = "" 
			var logStatisticsToHive:Boolean=false      
			var coordinatorName:String =""
			var alignDate:Boolean=false
        var groupId:             Integer  = -1
    var groupVersion:             Integer  = -1
			     
			     
    parser.parse( args, CliOptions()) match {
      case None =>
        // syntax error on command line
      case Some(opts) => {
        tenantCode=opts.tenantCode
          groupId = opts.groupId
          groupVersion = opts.groupVersion
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
			

		  LOG.info("[[HiveToSolrGruppiDs::main]] tenantCode --> "+tenantCode)
		  LOG.info("[[HiveToSolrGruppiDs::main]] groupId --> "+groupId)
		  LOG.info("[[HiveToSolrGruppiDs::main]] groupVersion --> "+groupVersion)
		  LOG.info("[[HiveToSolrGruppiDs::main]] toWrite --> "+toWrite)
		  LOG.info("[[HiveToSolrGruppiDs::main]] forceCollectionTO --> "+forceCollectionTO)
		  LOG.info("[[HiveToSolrGruppiDs::main]] logStatisticsToHive --> "+logStatisticsToHive)
		  LOG.info("[[HiveToSolrGruppiDs::main]] prjName --> "+prjName)
		  LOG.info("[[HiveToSolrGruppiDs::main]] coordinatorName --> "+coordinatorName)
		  LOG.info("[[HiveToSolrGruppiDs::main]] alignDate --> "+alignDate)
			
		  if (! (WRITEMODE_NOWRITE.equalsIgnoreCase(toWrite)  ||  WRITEMODE_APPEND.equalsIgnoreCase(toWrite) || WRITEMODE_OVERWRITE.equalsIgnoreCase(toWrite)  ) ) {
		    throw new Exception("invalid write mode '"+toWrite+"' !! valid values are nowrite, append, overwrite ") 
		  }
		  
			
			//TODO loggare parametri e descrivere configurazione e operazioni
			
			
			val conf = new SparkConf().set("spark.yarn.queue","produzione").setExecutorEnv(Array(("java.security.auth.login.config", "jaas-client.conf")));
			val sparkContext = new SparkContext(conf)
			val sqlContextHive=new org.apache.spark.sql.hive.HiveContext(sparkContext)

			val solrDelegate = new SolrDelegate(zkHost);
			val adminDel=new AdminApiDelegate()

			
			
			
			
			
			
			
      val listads=adminDel.getDatasetsByGroup(sparkContext, groupId, groupVersion, false, true)
			var dfStats:DataFrame = null;
    retCode = 0;
      for (v <- listads) {
        var ds =v
			      var dfCur=writeDataset(solrDelegate,sqlContextHive,sparkContext,ds,stepCnt,toWrite,forceCollectionTO,startFrom,prjName,logStatisticsToHive,coordinatorName )
			      if (alignDate==true)
			        adminDel.createOrUpdateAllineamento(ds.idDataset, ds.datasetVersion, "f48648f00000000000000000",ds.idOrganization)
			      LOG.info("[[HiveToSolrGruppiDs::::main]] PUHLISHED dataset "+ ds.datasetCode +"("+ds.idDataset+") dfCur.size="+dfCur.count())
			      //if (retCode<0) throw new Exception( " PUHLISHED  FAIL dataset "+ ds.datasetCode +"("+ds.idDataset+") retcode="+retCode)
			      if (retCode==0) dfStats=dfCur
			      else dfStats=dfStats.unionAll(dfCur)
			      retCode=retCode+1


      
      
      }			
			
			/*
			
			val arrDsId = eleDsId.split(',')
			var dfStats:DataFrame = null;
			retCode=0;
			for (v <- arrDsId) {
						var idDataset:Integer =Integer.parseInt(v)

			      var ds=adminDel.getDatasets4PromotionByIdDataset(sparkContext, idDataset)
			      var dfCur=writeDataset(solrDelegate,sqlContextHive,sparkContext,ds,stepCnt,toWrite,forceCollectionTO,startFrom,prjName,logStatisticsToHive,coordinatorName )
			      if (alignDate==true)
			        adminDel.createOrUpdateAllineamento(ds.idDataset, ds.datasetVersion, "f48648f00000000000000000",ds.idOrganization)
			      LOG.info("[[HiveToSolrGruppiDs::::main]] PUHLISHED dataset "+ ds.datasetCode +"("+ds.idDataset+") dfCur.size="+dfCur.count())
			      //if (retCode<0) throw new Exception( " PUHLISHED  FAIL dataset "+ ds.datasetCode +"("+ds.idDataset+") retcode="+retCode)
			      if (retCode==0) dfStats=dfCur
			      else dfStats=dfStats.unionAll(dfCur)
			      retCode=retCode+1
			      
      }
			if (logStatisticsToHive==true && dfStats!=null && dfStats.count()>0) {
			  dfStats.write.format("orc").mode("append").saveAsTable("db_csi_log.yuccadatalake_publish_logs")
			}
      */
		
		
		
	  } catch  {
	    case e: Exception => LOG.error("[[HiveToSolrGruppiDs::main]]", e)
	     throw e
	  } finally {
			LOG.info("[[HiveToSolrGruppiDs::main]] END")
	    
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
			
			LOG.info("[[HiveToSolrGruppiDs::main]]strCols = "+strCols)
			LOG.info("[[HiveToSolrGruppiDs::main]]solrCollection = "+solrCollection)
			LOG.info("[[HiveToSolrGruppiDs::main]]availableSpeed = "+ds.availableSpeed)
			LOG.info("[[HiveToSolrGruppiDs::main]]hive = "+ds.dbHiveSchema+"."+ds.dbHiveTable)
			

			//numero record iniziali su solr
      val countInitSolr = solrDelegate.countDocument(ds.idDataset, -1, solrCollection, null, null)
      val writeMode:SaveMode = toWrite.toLowerCase() match { 
					case "overwrite"   => SaveMode.Overwrite
					case "append" => SaveMode.Append
					case _ => SaveMode.Overwrite
			}		
			LOG.info("[[HiveToSolrGruppiDs::main]]   Conteggio iniziale record Solr --> "+countInitSolr)

      
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
			LOG.info("[[HiveToSolrGruppiDs::main]]   Numero Record hive da scrivere  --> "+numrecordTowrite)
      
			
			//CANCELLAZIONE IN CASO DI REPLACE
      var countAfterDele = countInitSolr
			if ( WRITEMODE_OVERWRITE.equalsIgnoreCase(toWrite)) {
			  LOG.info("[[HiveToSolrGruppiDs::main]]   deleting from hdfs --> tenant:" +ds.organizationCode + "   domain: "+ds.dataDomain  + "   subdomain: "+ ds.codSubDomain + " dssubtype:" +ds.subType + " dscode:" +ds.datasetCode+ " streamcode:" + ds.streamCode + " slug:" + ds.vESlug)
  			var hdfsHelper= new HDFSHelper(null)
			  //eliminata la cancellazione dei csv perhè ora viene eseguita nello scarico del csv
    		//if (!(WRITEMODE_NOWRITE.equalsIgnoreCase(toWrite))) hdfsHelper.deleteDataSet4promotion(sparkContext.hadoopConfiguration, ds.organizationCode, ds.dataDomain, ds.codSubDomain, ds.subType, ds.datasetCode, ds.streamCode, ds.vESlug)
				LOG.info("[[HiveToSolrGruppiDs::main]] deleting")
				if (!(WRITEMODE_NOWRITE.equalsIgnoreCase(toWrite))) solrDelegate.deleteDocsForPromotion(ds.idDataset, ds.datasetVersion, solrCollection)
				Thread.sleep(10000)
        countAfterDele = solrDelegate.countDocument(ds.idDataset, -1, solrCollection, null, null)
				LOG.info("[[HiveToSolrGruppiDs::main]] Conteggio record Solr dopo cancellazione cnt -->" +countAfterDele)
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
					
			LOG.info("[[HiveToSolrGruppiDs::main]] PHASE WRITING - START "+colsToSelect)
			do {
				var toRow=counter+stepCnt
						LOG.info("[[HiveToSolrGruppiDs::main]]      write from "+counter+" to "+ toRow +" of " + numrecordTowrite)

						var dfTowrite=hiveTable.filter(col("sparkuniqueid") >= lit(counter) ).filter(hiveTable.col("sparkuniqueid") <= lit(toRow) ).select(colsToSelect.head, colsToSelect.tail: _*)


						if (!(WRITEMODE_NOWRITE.equalsIgnoreCase(toWrite))) {
						  //dfTowrite.write.format("solr").options(solroptions).mode(writeMode).save
						  dfTowrite.write.format("org.csi.yucca.storageutils.helper.solrformat").options(solroptions).mode(writeMode).save
						  
						  
						  LOG.info("[[HiveToSolrGruppiDs::main]]      block done "+dfTowrite.count())
						} else {
						  LOG.info("[[HiveToSolrGruppiDs::main]]      block skipped - nowrite ")
						  
						}
						Thread.sleep(10000) 
						LOG.info("[[HiveToSolrGruppiDs::main]]      sleep done")

						counter=counter+stepCnt
						cicli=cicli+1 
			} while (counter < numrecordTowrite && cicli <150)
			LOG.info("[[HiveToSolrGruppiDs::main]] PHASE WRITING - END ")
			
			//Tempo trascorso per scrittura dati su Solr
			val elapsedTime = System.currentTimeMillis - start
			
      var countAfterWriting = solrDelegate.countDocument(ds.idDataset, ds.datasetVersion, solrCollection, null, null)
			var recWritten=countAfterWriting-countAfterDele
			LOG.info("[[HiveToSolrGruppiDs::main]] Conteggio record Solr dopo scrittura cnt -->" +countAfterWriting)
			LOG.info("[[HiveToSolrGruppiDs::main]] WRITE DONE "+ds.datasetCode +"("+ds.idDataset+ ") Scritti  "+recWritten+" di "+numrecordTowrite)
      
			
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
  			LOG.warn("[[HiveToSolrGruppiDs::main]] quadratura errata delta" + (numrecordTowrite-recWritten))
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