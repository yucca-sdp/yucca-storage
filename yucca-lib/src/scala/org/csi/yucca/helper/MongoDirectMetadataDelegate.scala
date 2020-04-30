package org.csi.yucca.helper

import org.slf4j.LoggerFactory
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import org.apache.spark.SparkContext
import org.bson.Document
import org.apache.spark.rdd.RDD
import com.mongodb.spark.rdd.MongoRDD
import com.mongodb.spark.config.WriteConfig
import com.mongodb.spark.MongoConnector
import org.apache.spark.sql.Row

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import com.mongodb.MongoClient
import com.mongodb.Block
import com.mongodb.client.model.Filters._
import com.mongodb.client.model.Filters
import com.mongodb.MongoClientURI
import org.bson.types.ObjectId
import com.mongodb.client.model.UpdateOptions

import java.util.ArrayList
import scala.collection.mutable.ListBuffer
import collection.JavaConverters._

import org.csi.yucca.dto._

class MongoDirectMetadataDelegate(val connectionUrl: String) {
  val configuration = Map(
    "uri" -> (connectionUrl + "&readPreference=primaryPreferred"),
    "partitioner" -> "MongoPaginateBySizePartitioner")

  val configurationOutput = Map(
    "uri" -> (connectionUrl + "&readPreference=primaryPreferred"),
    "partitioner" -> "MongoPaginateBySizePartitioner")

  val LOG = LoggerFactory.getLogger(getClass)
  val mongoClient: MongoClient = new MongoClient(
      new MongoClientURI(connectionUrl)
      )

  def close() = {
    mongoClient.close();
  }

  def addToArray(array: Array[Document], doc: Document): Array[Document] =
    {
      return array :+ doc
    }

  def getTenants(sparkContext: SparkContext): Array[Document] = {
    LOG.info("[[MongoMetadataDelegate::getTenants]] Starting get tenants")
    var tenantColl = mongoClient.getDatabase("DB_SUPPORT").getCollection("tenant")
    var docs = Array[Document]()

    var iter = tenantColl.find()

    iter.forEach(
      new Block[Document]() {
        @Override
        def apply(document: Document) {
          docs = addToArray(docs, document)
        }
      })
    docs
  }

  
	def getTenantByCode(sparkContext: SparkContext, tenantCode: String) : TenantInfo = {
    LOG.info("[[MongoMetadataDelegate::getTenantByCode]] Starting getDatasetNotDeletedByTenantCode")
    var tenantColl = mongoClient.getDatabase("DB_SUPPORT").getCollection("tenant")
    var docs = Array[Document]()
    var iter = tenantColl.find(Filters.eq("tenantCode", tenantCode))

    iter.forEach(
      new Block[Document]() {
        @Override
        def apply(document: Document) {
          docs = addToArray(docs, document)
        }
      })

    val tenant=docs(0)  
    var ret:TenantInfo=new TenantInfo();
			
		ret.tenantCode=tenantCode
		ret.solrCollection_DATA  =  tenant.getString("dataSolrCollectionName")
	  ret.solrCollection_MEASURES =  tenant.getString("measuresSolrCollectionName")
		ret.solrCollection_SOCIAL =  tenant.getString("socialSolrCollectionName")
		ret.solrCollection_MEDIA =  tenant.getString("mediaSolrCollectionName")
		ret.organizationCode=  tenant.getString("organizationCode")
    LOG.info("[[MongoMetadataDelegate::getTenantByCode]] loaded ")
		return ret

	}
  
  
  
  def getDatasetNotDeletedByTenantCode(sparkContext: SparkContext, tenantCode: String): Array[Document] = {
    LOG.info("[[MongoMetadataDelegate::getDatasetNotDeletedByTenantCode]] Starting getDatasetNotDeletedByTenantCode")

    var datasetColl = mongoClient.getDatabase("DB_SUPPORT").getCollection("metadata")
    var docs = Array[Document]()

    var iter = datasetColl.find(
      Filters.and(
        Filters.eq("configData.tenantCode", tenantCode),
        Filters.ne("configData.subtype", "binaryDataset"),
        Filters.or(
          Filters.exists("configData.deleted", false),
          Filters.eq("configData.deleted", 0))))

    iter.forEach(
      new Block[Document]() {
        @Override
        def apply(document: Document) {
          docs = addToArray(docs, document)
        }
      })
    LOG.info("[[MongoMetadataDelegate::getDatasetNotDeletedByTenantCode]] loaded ")
    docs
  }

  def getLastObjectIdByTenantCode(sparkContext: SparkContext, tenantCode: String): String = {
    var allineamnetoColl = mongoClient.getDatabase("DB_SUPPORT").getCollection("allineamento2")
    var last = "000000000000000000000000"
    var doc = allineamnetoColl.find(
      Filters.and(
        Filters.eq("tenantCode", tenantCode),
        Filters.exists("last_objectid", true))).first()

    if (null != doc) {
      last = doc.getObjectId("last_objectid").toString()
    }
    return last
  }

  def getStreamByTenantCodeIdDatasetDatasetVersione(sparkContext: SparkContext, tenantCode: String, idDataset: Integer, datasetVersion: Integer) : Array[Document] = {
    LOG.info("[[MongoMetadataDelegate::getStreamByTenantCodeIdDatasetDatasetVersione]] Starting getDatasetNotDeletedByTenantCode")
    var streamsRDD = MongoSpark.load(
      sparkContext,
      ReadConfig(Map("collection" -> "stream", "readPreference.name" -> "primaryPreferred") ++ configuration))
    streamsRDD.withPipeline(Seq(
      Filters.and(
        Filters.eq("configData.tenantCode", tenantCode),
        Filters.eq("configData.idDataset", idDataset),
        Filters.eq("configData.datasetVersion", datasetVersion)
      )
    )).asInstanceOf[Array[Document]]
  }

  def getStreamByTenantCode(sparkContext: SparkContext, tenantCode: String): Array[Document] = {
    LOG.info("[[MongoMetadataDelegate::getStreamByTenantCode]] Starting getDatasetNotDeletedByTenantCode")

    var streamColl = mongoClient.getDatabase("DB_SUPPORT").getCollection("stream")
    var docs = Array[Document]()

    var iter = streamColl.find(
      Filters.eq("configData.tenantCode", tenantCode))
    iter.forEach(
      new Block[Document]() {
        @Override
        def apply(document: Document) {
          docs = addToArray(docs, document)
        }
      })

    LOG.info("[[MongoMetadataDelegate::getStreamByTenantCode]] loaded ")
    return docs
  }

  def updateLastObjectIdByTenantCode(sparkContext: SparkContext, tenantCode: String, updatedObjectId: String) {

    var allineamnetoColl = mongoClient.getDatabase("DB_SUPPORT").getCollection("allineamento2")

    var doc = allineamnetoColl.find(
        Filters.eq("tenantCode", tenantCode)).first()

         // TODO check if null
    if (doc == null)
    {
      doc = new Document
      doc.put("tenantCode", tenantCode)
      doc.put("locked", 0)
    }
        
        
    doc.put("last_objectid", new ObjectId(updatedObjectId))
        
    var upd = new UpdateOptions()
    upd.upsert(true)
    allineamnetoColl.replaceOne(Filters.eq("tenantCode", tenantCode), doc,  upd)


    LOG.info("[[MongoMetadataDelegate::updateLastObjectIdByTenantCode]] updated  " + tenantCode + " with " + updatedObjectId)
  }

 	def getAllDatasets4PromotionByTenantcode(sc : SparkContext, tenantCode: String) : List[DatasetInfo] = {
 	  LOG.info("[[MongoMetadataDelegate::getAllDatasets4PromotionByTenantcode]] Starting getAllDatasets4PromotionByTenantcode")
    var metadataColl = mongoClient.getDatabase("DB_SUPPORT").getCollection("metadata")
    var docs = Array[Document]()
    var iter = metadataColl.find(
        Filters.and(
          Filters.eq("configData.tenantCode", tenantCode),
          Filters.eq("availableSpeed",  true ),
          Filters.ne("configData.subtype", "binaryDataset"),
          Filters.eq("configData.current", 1),
          Filters.or(
              Filters.exists("configData.deleted", false),
              Filters.eq("configData.deleted",  0 )
              
          )
      )
    
    )
    var ret:ListBuffer[DatasetInfo]=new ListBuffer[DatasetInfo]
    iter.forEach(
      new Block[Document]() {
        @Override
        def apply(document: Document) {
          ret+=castDatasetdocumentToDatasetInfo(sc, document)
        }
      })

    LOG.info("[[MongoMetadataDelegate::getAllDatasets4PromotionByTenantcode]] loaded ")
    return ret.toList    
 	}
	def getDatasets4PromotionByIdDataset(sc : SparkContext, tenantCode: String, idDataset:Integer) : DatasetInfo = {
 	  LOG.info("[[MongoMetadataDelegate::getDatasets4PromotionByIdDataset]] Starting getDatasets4PromotionByIdDataset")
    var metadataColl = mongoClient.getDatabase("DB_SUPPORT").getCollection("metadata")
    var docs = Array[Document]()
    var iter = metadataColl.find(
        Filters.and(
          Filters.eq("idDataset", idDataset),
          Filters.eq("configData.tenantCode", tenantCode),
          Filters.eq("availableSpeed",  true ),
          Filters.ne("configData.subtype", "binaryDataset"),
          Filters.eq("configData.current", 1),
          Filters.or(
              Filters.exists("configData.deleted", false),
              Filters.eq("configData.deleted",  0 )
              
          )
      )
    
    )
    
    var dsinfo: DatasetInfo = castDatasetdocumentToDatasetInfo(sc, iter.first())

    LOG.info("[[MongoMetadataDelegate::getDatasets4PromotionByIdDataset]] loaded ")
	  return dsinfo 
	}

	def castDatasetdocumentToDatasetInfo(sc : SparkContext, ds: Document) : DatasetInfo = {
	      LOG.info("[[MongoMetadataDelegate::castDatasetdocumentToDatasetInfo]] ds= "+ds)

	  var curds:DatasetInfo= new DatasetInfo()

		  curds.availableSpeed=ds.getBoolean("availableSpeed")
			curds.datasetVersion = ds.getInteger("datasetVersion")
			curds.idDataset = ds.getInteger("idDataset")
			curds.datasetCode = ds.getString("datasetCode")			
			curds.subType = ds.get("configData").asInstanceOf[Document].getString("subtype")
			curds.tenantCode= ds.get("configData").asInstanceOf[Document].getString("tenantCode")
			
			
			curds.dbHiveSchema=ds.getString("dbHiveSchema")
			curds.dbHiveTable=ds.getString("dbHiveTable")
			var fields = ds.get("info").asInstanceOf[Document].get("fields").asInstanceOf[ArrayList[Document]].asScala
      curds.campi=fields.map( field =>
			   new Field(field.getString("fieldName"),field.getString("dataType"))
					).toList		
      curds.dataDomain = ds.get("info").asInstanceOf[Document].getString("dataDomain")
      curds.codSubDomain = ds.get("info").asInstanceOf[Document].getString("codSubDomain")
      if (curds.codSubDomain == null || curds.codSubDomain == "") {
        curds.codSubDomain = curds.codSubDomain
      }
	     
	  
  		var streamCode:String=null
  		var vESlug:String=null
      if (curds.subType.equals("streamDataset") || curds.subType.equals("socialDataset")) {
            val sluggedStreams = getStreamByTenantCodeIdDatasetDatasetVersione(sc, curds.tenantCode, curds.idDataset, curds.datasetVersion)

            if (sluggedStreams.size > 0) {
              //vESlug = sluggedStreams.first.get("streams").asInstanceOf[Document].get("stream").asInstanceOf[Document].getString("virtualEntitySlug")
              vESlug = sluggedStreams(0).get("streams").asInstanceOf[Document].get("stream").asInstanceOf[Document].getString("virtualEntitySlug")
              if (vESlug != null && vESlug != "") // sadly, that may happen to us (!)
                vESlug = vESlug.replaceAll("[^a-zA-Z0-9-_]", "-")
              streamCode = sluggedStreams(0).get("streamCode").asInstanceOf[String]
            }
        
      }
  		
  		curds.streamCode=streamCode
  		curds.vESlug=vESlug
	  
	  return curds
	}  
  
}

