package it.csi.compactor

import java.util.concurrent.Callable
import java.util.concurrent.ExecutorCompletionService
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.immutable
import scala.collection.mutable.Buffer
import scala.util.control.Breaks.break
import scala.util.control.Breaks.breakable
import org.apache.commons.csv.QuoteMode
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.FileUtil
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.coalesce
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.date_format
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.to_utc_timestamp
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.StringType
import org.csi.yucca.adminapi.client.BackOfficeCreateClient
import org.csi.yucca.adminapi.client.BackofficeListaClient
import org.csi.yucca.adminapi.response.AllineamentoScaricoDatasetResponse
import org.csi.yucca.adminapi.response.BackofficeDettaglioStreamDatasetResponse
import org.csi.yucca.adminapi.response.OrganizationResponse
import org.slf4j.LoggerFactory

import java.io.File
import java.util.Calendar
import java.util.Date
import scala.collection.mutable.HashMap
import org.bson.types.ObjectId
import org.apache.commons.io.filefilter.WildcardFileFilter


class MT_dataSetsCompactor(
    organization: OrganizationResponse,
    datasetCode: String,
    sqlContext: SQLContext,
    sparkContext: SparkContext,
    adminApiUrl: String,
    objectId: String) extends Callable[Outcomes] {

  val LOG = LoggerFactory.getLogger(getClass)

  def getListStreamDataset(organizationCode: String): Option[List[BackofficeDettaglioStreamDatasetResponse]] = {
    try {
      val result = BackofficeListaClient.getListStreamDataset(adminApiUrl, organizationCode, "MultiThreadDownloadCSV")
      if (result != null)
        Some(result.asScala.toList)
      else
        Some(List.empty[BackofficeDettaglioStreamDatasetResponse])
    } catch {
      case t: Throwable => None
    }
  }


  def submitDatasetsAndShutdown(
    pool : ExecutorService,
    datasets : List[BackofficeDettaglioStreamDatasetResponse]) = {

    val completionService = new ExecutorCompletionService[Outcomes](pool)

    var count = 0
    for 
    (
        streamDataset <- datasets if ((datasetCode == null || datasetCode.equals(streamDataset.getDataset.getDatasetcode)) && (streamDataset.getDataset.getDatasetSubtype.getDatasetSubtype != "bulkDataset") && (streamDataset.getDataset.getDatasetSubtype.getDatasetSubtype != "binaryDataset"))
    ) 
    {
        completionService.submit ( new SparkDatasetsCompactor(streamDataset, sparkContext, sqlContext, adminApiUrl,objectId))
        count = count + 1
    }
    LOG.debug("MultiThreadCompactCSV.call() ==> AFTER task submit("+organization.getOrganizationcode+")")

    pool.shutdown();        // ensure everything goes down when all datasets have been "processed"

    LOG.debug("MultiThreadCompactCSV.call() ==> AFTER pool shutdown("+organization.getOrganizationcode+")")
    var progress = 0
    while (!pool.isTerminated() && progress < count) {
      val ahead = completionService.poll(10, TimeUnit.MILLISECONDS)
      if (ahead != null) {
        if (ahead.isDone() || ahead.isCancelled()) {
          val outcomes = ahead.get()
          progress = progress + 1
          if (outcomes.hasErrors) {
            LOG.error("[[CompactCSV::main]] ERROR on ORGANIZATION " + organization.getOrganizationcode() + " (" + outcomes.errorMessage + ")")
          } else {
            LOG.info("[[CompactCSV::main]] ORGANIZATION " + organization.getOrganizationcode() + " IN PROGRESS(" + progress + "/" + count + ")")
          }
        }
      }
    }

    if (!pool.awaitTermination(4, TimeUnit.HOURS)) {
      pool.shutdownNow();
    }
  }

  def call(): Outcomes = {
    LOG.debug("MultiThreadCompactCSV.call() ==> BEGIN")
    LOG.info("PROCESS ORGANIZATION: " + organization.getOrganizationcode)

    var errorMessage = ""

    try {
      val pool = Executors.newFixedThreadPool(10);
      LOG.debug("MultiThreadCompactCSV.call() ==> Executor READY ("+organization.getOrganizationcode+")")

      getListStreamDataset(organization.getOrganizationcode()) match {
        case Some(streamDatasets) => {
          LOG.debug("MultiThreadCompactCSV.call() ==> BEFORE task submit("+organization.getOrganizationcode+")")
          submitDatasetsAndShutdown(pool, streamDatasets)
          LOG.info("<<<<<<<<<<<<<<<<<<<<<<" + organization.getOrganizationcode + ">>>>>>>>>>>>>>> END")
        }
        case None => {
          LOG.warn("No stream datasets for orgId" + organization.getOrganizationcode)
        }
      }

    } catch {
      case ex: Exception => {
        LOG.error("Errore durante organization " + organization.getOrganizationcode, ex)
        errorMessage = ex.getMessage
      }
    }

    return Outcomes(organization.getOrganizationcode, errorMessage, !errorMessage.isEmpty())
  }

}

class SparkDatasetsCompactor(
    streamDataset: BackofficeDettaglioStreamDatasetResponse,
    sparkContext: SparkContext,
    sqlContext: SQLContext,
    adminApiUrl: String,
    objectId:  String) extends Callable[Outcomes] {

  val LOG = LoggerFactory.getLogger(getClass)

  def getDsType(subType: String) = {
    val dsType = subType match {
      case "bulkDataset"   => "data"
      case "socialDataset" => "social"
      case _               => "measures"
    }
    dsType
  }


  def getSubdomainCode(streamDataset: BackofficeDettaglioStreamDatasetResponse, dataDomain: String) = {
    var codSubDomain = streamDataset.getSubdomain.getSubdomaincode
    if (codSubDomain == null || codSubDomain == "") {
      codSubDomain = dataDomain
    }
    codSubDomain
  }


  def getVESlug(dsType: String) = {
    var vESlug = ""

    if (dsType != "data") {

      vESlug = streamDataset.getStream.getSmartobject.getSlug

      if (vESlug != null && vESlug != "") {
        vESlug = vESlug.replaceAll("[^a-zA-Z0-9-_]", "-")
      }
    }
    vESlug
  }

  def getStreamCode(dsType: String) = {

    var streamCode = ""

    if (dsType != "data") {
      streamCode = streamDataset.getStream.getStreamcode
    }

    streamCode
  }


  def getFinalDir(subType: String, organizationCode: String, dataDomain: String, codSubDomain: String, vESlug: String) = {
    var tempFinalDir = "/datalake/" + organizationCode.toUpperCase() + "/rawdata/" + dataDomain.toUpperCase() + "/"
    var finalDir = subType match {
      case "bulkDataset" => tempFinalDir + "db_" + codSubDomain.toUpperCase()
      case _             => tempFinalDir + "so_" + vESlug
    }

    finalDir
  }

  def getLastDir(subType: String, datasetCode: String, streamCode: String) = {
    var lastDir = subType match {
      case "bulkDataset" => datasetCode
      case _             => streamCode
    }

    lastDir
  }

  def mkdirs(fs: FileSystem, finalDir: String, lastDir: String) = {

    LOG.info("ThreadDatasetDownloadCSV.call() ==> BEGIN")

    LOG.info("finalDir: " + finalDir)
    LOG.info("lastDir: " + lastDir)

    if (!(fs.exists(new Path(finalDir)))) {
      fs.mkdirs(new Path(finalDir), new FsPermission("755"))
    }

    if (!(fs.exists(new Path(finalDir + "/" + lastDir)))) {
      fs.mkdirs(new Path(finalDir + "/" + lastDir), new FsPermission("750"))
    }

    LOG.info("ThreadDatasetDownloadCSV.call() ==> END")
  }


  def call(): Outcomes = {

    LOG.debug("ThreadDatasetCompactorCSV.call() ==> BEGIN")
    LOG.info("ThreadDatasetCompactorCSV.call() idDataset [ " + streamDataset.getDataset.getIddataset + " ]")
    import sqlContext.implicits._
    sparkContext.hadoopConfiguration.set("mapred.output.compress", "false")
     
    var error = ""
    try {

      breakable {
       val datasetType = getDsType(streamDataset.getDataset.getDatasetSubtype.getDatasetSubtype)
       
       var finalDir = getFinalDir(streamDataset.getDataset.getDatasetSubtype.getDatasetSubtype, streamDataset.getOrganization.getOrganizationcode, streamDataset.getDomain.getDomaincode, getSubdomainCode(streamDataset, streamDataset.getDomain.getDomaincode), getVESlug(datasetType))
       var lastDir = getLastDir(streamDataset.getDataset.getDatasetSubtype.getDatasetSubtype, streamDataset.getDataset.getDatasetcode, getStreamCode(datasetType))
       val fileSystem = FileSystem.get(sparkContext.hadoopConfiguration)

       val cal = Calendar.getInstance()

       val fileMap = HashMap[String, Array[String]]()
    
       var files = Array[FileStatus]()
       if (fileSystem.exists(new Path(finalDir + "/" + lastDir))) {
         files =  fileSystem.listStatus(new Path(finalDir+ "/" +lastDir))
         files = files.filter(_.getPath.getName.endsWith("-" + streamDataset.getDataset.getDatasetcode + "-" + streamDataset.getVersion + ".csv" )) 
       }
           
      
       var _compactorTmpPath = "tmp/compactor/"+ objectId + "/" 
                
       // Popolo la mappa con la lista dei file
       // key --> anno_mese_versione (N.B. L'anno ed il mese sono quelli dell'objectId contenuto nel nome File)
       // value --> lista di tutti file con stesso anno/mese/versione
       
       if (!files.isEmpty) {
         for(file <-files){
           
             //ObjectID del file elaborato
             val fileObjectID = new ObjectId(file.getPath.getName.split("_")(0))
             cal.setTime(fileObjectID.getDate)
             
             val monthModify = cal.get(Calendar.MONTH)+1;
             val yearModify = cal.get(Calendar.YEAR )           
             val version = streamDataset.getVersion
             
             val key = yearModify + "_" + monthModify + "_" + version           
             
             if(fileMap.contains(key)) {             
               fileMap(key) = fileMap(key):+file.getPath.getName
             }           
             else fileMap += (key -> Array(file.getPath.getName))
          }
       }  
        
        if (fileMap.size > 0)
        {
          for ((key,value) <- fileMap)     
           {
          val annoElab = key.split("_")(0)
          val meseElab = key.split("_")(1)
          var tmpFileDir = _compactorTmpPath + streamDataset.getDataset.getDatasetcode + "/" + annoElab + "_" + meseElab + "/" + streamDataset.getVersion + "/"
          var maxObjectId = "";
          if (value.length > 1) {
              // val dfFirst = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load(finalDir+ "/" +lastDir+ "/" + value(0))
              // val schema = dfFirst.schema
              // LOG.info("SCHEMA" + schema)
               for (vfile <- value) {                 
                   FileUtil.copy(fileSystem,new Path(finalDir+ "/" +lastDir+ "/" + vfile),fileSystem,new Path(tmpFileDir +  vfile),true,sparkContext.hadoopConfiguration)
                   val nameSplittedForObjectID = vfile.split("_")
                   val ObjectIDFile = nameSplittedForObjectID(0)
                   if (ObjectIDFile > maxObjectId) maxObjectId = ObjectIDFile
               }          
                 
              LOG.info("Mese in elaborazione " + key)
              val df = sqlContext.read.format("com.databricks.spark.csv")
                        .option("header", "true")
                        .load(tmpFileDir +"/*.csv")

              val dfFiltered = datasetType match {
                case "bulkDataset"   =>  df
                case "binaryDataset" =>  df
                case _               =>  df.filter(!(df("time")===("time")))
              }
              
              //val dfFiltered = df.filter(!(df("time")===("time")))
              
              dfFiltered.show()
              
              val countBigText =  dfFiltered.count()
              
              var __compacttmpfilename = maxObjectId + "_" + countBigText + "-" + streamDataset.getDataset.getDatasetcode + "-" + streamDataset.getVersion + ".csv.tmp"
              
              dfFiltered.coalesce(1).write.format("com.databricks.spark.csv").
                option("header", "true").
                option("quoteMode", QuoteMode.MINIMAL.toString).
                option("treatEmptyValuesAsNulls", "false").
                mode("overwrite").
                save(tmpFileDir +  __compacttmpfilename)
                
              var fileName = fileSystem.globStatus(new Path(tmpFileDir + __compacttmpfilename + "/part*"))(0).getPath.getName
              var compactfinalfilename = maxObjectId + "_" + countBigText + "-" + streamDataset.getDataset.getDatasetcode + "-" + streamDataset.getVersion + ".csv"
                
              LOG.info("FILECOMPACTSOURCE:" + tmpFileDir + __compacttmpfilename + "/" + fileName)
              LOG.info("FILECOMPACTDEST:" + finalDir + "/" + lastDir + "/"  + compactfinalfilename)
              
              fileSystem.rename(new Path(tmpFileDir + __compacttmpfilename + "/" +  fileName), new Path(finalDir + "/" + lastDir + "/"  + compactfinalfilename))
              
              //Elimino cartella temporanea
              //if  (fileSystem.exists(new Path(_compactorTmpPath +  mongoObjectId + "/" + lastDir + "/" + streamDataset.getVersion)))
                //fileSystem.delete(new Path(_compactorTmpPath + mongoObjectId + "/" + lastDir + "/" + streamDataset.getVersion),true)
             }
           }
        }
      }

    } catch {
      case ex: Exception => {
        LOG.error("Errore ("+ex.getMessage+") durante l'organization " + streamDataset.getOrganization.getOrganizationcode + ", dataset:" + streamDataset.getDataset.getDatasetcode, ex)
        error = ex.getMessage + "-"
      }
    }

    return Outcomes(streamDataset.getDataset.getDatasetcode + "_" + streamDataset.getVersion, error, !error.isEmpty())

  }

}
