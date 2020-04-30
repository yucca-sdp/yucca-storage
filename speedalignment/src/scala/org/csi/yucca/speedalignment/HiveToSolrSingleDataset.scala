package org.csi.yucca.storageutils.speedalignment


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

import org.csi.yucca.storageutils.helper.HDFSHelper
import org.csi.yucca.storageutils.dto.DatasetInfo
import org.csi.yucca.storageutils.dto.TenantInfo
import org.csi.yucca.storageutils.helper.SorlDelegate

import org.csi.yucca.storageutils.helper.MongoDirectMetadataDelegate
import com.lucidworks.spark.util.ConfigurationConstants





object HiveToSolrSingleDataset {
  
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
  
  
  
  val zkHost: String = ""	
  
	def main(args:Array[String]) = {
    var retCode:Integer = -1
  try {
    			System.setProperty("java.security.auth.login.config", "jaas-client.conf");
			System.setProperty("spark.driver.allowMultipleContexts", "true");
			System.setProperty("solr.jaas.conf.path","jaas-client.conf");

    
		LOG.info("[[HiveToSolrSingleDataset::main]] BEGIN")

		
			val tenantCode: String = args(0)
			val idDataset:Integer =Integer.parseInt(args(1))
			val toWrite: String = args(2) //nowrite //append //replace
			val forceCollectionTO: String =args(3) //none per usare collection di default
			
			val mongoConnection: String =args(4) //discovery:
			val startFrom:Integer =Integer.parseInt(args(5))
			
			val stepCnt: Integer = 100000
			

		  LOG.info("[[HiveToSolrSingleDataset::main]] tenantCode --> "+tenantCode)
		  LOG.info("[[HiveToSolrSingleDataset::main]] idDataset --> "+idDataset)
		  LOG.info("[[HiveToSolrSingleDataset::main]] toWrite --> "+toWrite)
		  LOG.info("[[HiveToSolrSingleDataset::main]] forceCollectionTO --> "+forceCollectionTO)
		  LOG.info("[[HiveToSolrSingleDataset::main]] mongoConnection --> "+mongoConnection)
		  LOG.info("[[HiveToSolrSingleDataset::main]] startFrom --> "+startFrom)
			
		  if (! (WRITEMODE_NOWRITE.equalsIgnoreCase(toWrite)  ||  WRITEMODE_APPEND.equalsIgnoreCase(toWrite) || WRITEMODE_OVERWRITE.equalsIgnoreCase(toWrite)  ) ) {
		    throw new Exception("invalid write mode '"+toWrite+"' !! valid values are nowrite, append, overwrite ") 
		  }
		  
			
			//TODO loggare parametri e descrivere configurazione e operazioni
			
			
			val conf = new SparkConf().set("spark.yarn.queue","produzione").setExecutorEnv(Array(("java.security.auth.login.config", "jaas-client.conf")));
			val sparkContext = new SparkContext(conf)
			val sqlContextHive=new org.apache.spark.sql.hive.HiveContext(sparkContext)

			val solrDelegate = new SorlDelegate(zkHost);
		
		
			val mongoDel=new MongoDirectMetadataDelegate("mongodb://"+mongoConnection+"/DB_SUPPORT.allineamento2?authSource=admin")

		
			val tenant=mongoDel.getTenantByCode(sparkContext, tenantCode)
			
			val ds=mongoDel.getDatasets4PromotionByIdDataset( tenantCode, idDataset)
		
		
			
			retCode=writeDataset(solrDelegate,sqlContextHive,sparkContext,ds,tenant,stepCnt,toWrite,forceCollectionTO,startFrom)
		
		
		
	  } catch  {
	    case e: Exception => LOG.error("[[HiveToSolrSingleDataset::main]]", e)
	    System.exit(-1)
	  } finally {
			LOG.info("[[HiveToSolrSingleDataset::main]] END")
	    
	  }
	  
    System.exit(retCode)
	  
	}
  
  
  
  
  
  
  
  
  def writeDataset(solrDelegate:SorlDelegate,sqlContextHive:HiveContext,sparkContext:SparkContext,
      ds:DatasetInfo,tenant: TenantInfo,
      stepCnt:Integer,
      toWrite:String,
      forceCollectionTO: String,
      startFrom:Integer) : Integer = {
    
      val dsType = ds.subType match { 
					case "bulkDataset"   => "data"
					case "socialDataset" => "social"
					case _ => "measures"
			}				

    
			var solrCollection = getSolrCollection(ds,tenant,forceCollectionTO,dsType)
			
			
			var strCols=getSolrColumns(ds,dsType)
			
			LOG.info("[[HiveToSolrSingleDataset::main]]strCols = "+strCols)
			LOG.info("[[HiveToSolrSingleDataset::main]]solrCollection = "+solrCollection)
			LOG.info("[[HiveToSolrSingleDataset::main]]availableSpeed = "+ds.availableSpeed)
			LOG.info("[[HiveToSolrSingleDataset::main]]hive = "+ds.dbHiveSchema+"."+ds.dbHiveTable)
			

			//numero record iniziali su solr
      val countInitSolr = solrDelegate.countDocument(ds.idDataset, -1, solrCollection, null, null)
      val writeMode:SaveMode = toWrite.toLowerCase() match { 
					case "overwrite"   => SaveMode.Overwrite
					case "append" => SaveMode.Append
					case _ => SaveMode.Overwrite
			}		
			LOG.info("[[HiveToSolrSingleDataset::main]]   Conteggio iniziale record Solr --> "+countInitSolr)

      
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
			LOG.info("[[HiveToSolrSingleDataset::main]]   Numero Record hive da scrivere  --> "+numrecordTowrite)
      
			
			//CANCELLAZIONE IN CASO DI REPLACE
      var countAfterDele = countInitSolr
			if ( WRITEMODE_OVERWRITE.equalsIgnoreCase(toWrite)) {
			  LOG.info("[[HiveToSolrSingleDataset::main]]   deleting from hdfs --> tenant:" +tenant.organizationCode + "   domain: "+ds.dataDomain  + "   subdomain: "+ ds.codSubDomain + " dssubtype:" +ds.subType + " dscode:" +ds.datasetCode+ " streamcode:" + ds.streamCode + " slug:" + ds.vESlug)
  			var hdfsHelper= new HDFSHelper(null)
			  //eliminata la cancellazione dei csv perhè ora viene eseguita nello scarico del csv
    		//if (!(WRITEMODE_NOWRITE.equalsIgnoreCase(toWrite))) hdfsHelper.deleteDataSet4promotion(sparkContext.hadoopConfiguration, tenant.organizationCode, ds.dataDomain, ds.codSubDomain, ds.subType, ds.datasetCode, ds.streamCode, ds.vESlug)
				LOG.info("[[HiveToSolrSingleDataset::main]] deleting")
				if (!(WRITEMODE_NOWRITE.equalsIgnoreCase(toWrite))) solrDelegate.deleteDocsForPromotion(ds.idDataset, ds.datasetVersion, solrCollection)
				Thread.sleep(10000)
        countAfterDele = solrDelegate.countDocument(ds.idDataset, -1, solrCollection, null, null)
				LOG.info("[[HiveToSolrSingleDataset::main]] Conteggio record Solr dopo cancellazione cnt -->" +countAfterDele)
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
    			
    			
					
			LOG.info("[[HiveToSolrSingleDataset::main]] PHASE WRITING - START "+colsToSelect)
			do {
				var toRow=counter+stepCnt
						LOG.info("[[HiveToSolrSingleDataset::main]]      write from "+counter+" to "+ toRow +" of " + numrecordTowrite)

						var dfTowrite=hiveTable.filter(col("sparkuniqueid") >= lit(counter) ).filter(hiveTable.col("sparkuniqueid") <= lit(toRow) ).select(colsToSelect.head, colsToSelect.tail: _*)


						if (!(WRITEMODE_NOWRITE.equalsIgnoreCase(toWrite))) {
						  //dfTowrite.write.format("solr").options(solroptions).mode(writeMode).save
						  dfTowrite.write.format("org.csi.yucca.storageutils.helper.solrformat").options(solroptions).mode(writeMode).save
						  
						  
						  LOG.info("[[HiveToSolrSingleDataset::main]]      block done "+dfTowrite.count())
						} else {
						  LOG.info("[[HiveToSolrSingleDataset::main]]      block skipped - nowrite ")
						  
						}
						Thread.sleep(10000) 
						LOG.info("[[HiveToSolrSingleDataset::main]]      sleep done")

						counter=counter+stepCnt
						cicli=cicli+1 
			} while (counter < numrecordTowrite && cicli <90)
			LOG.info("[[HiveToSolrSingleDataset::main]] PHASE WRITING - END ")
      var countAfterWriting = solrDelegate.countDocument(ds.idDataset, ds.datasetVersion, solrCollection, null, null)
			var recWritten=countAfterWriting-countAfterDele
			LOG.info("[[HiveToSolrSingleDataset::main]] Conteggio record Solr dopo scrittura cnt -->" +countAfterWriting)
			LOG.info("[[HiveToSolrSingleDataset::main]] Scritti  "+recWritten+" di "+numrecordTowrite)
      if (recWritten!=numrecordTowrite)	{
  			LOG.warn("[[HiveToSolrSingleDataset::main]] quadratura errata delta" + (numrecordTowrite-recWritten))
  			return -1
      } 
			return 0
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
			val strColsTmp=ds.campi.map( field =>
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