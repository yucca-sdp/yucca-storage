package org.csi.yucca.helper

import org.slf4j.LoggerFactory
import org.apache.spark.SparkContext
import org.bson.Document
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema

import org.bson.types.ObjectId


import java.util.ArrayList
import scala.collection.mutable.ListBuffer
import collection.JavaConverters._

import org.csi.yucca.dto._
import org.csi.yucca.adminapi.client.BackofficeListaClient
import org.csi.yucca.adminapi.client.BackofficeDettaglioClient
import org.csi.yucca.adminapi.response.BackofficeDettaglioStreamDatasetResponse
import org.csi.yucca.adminapi.client.BackOfficeCreateClient
import scala.collection.JavaConverters._

class AdminApiDelegate {
   val LOG = LoggerFactory.getLogger(getClass)
   val ADMIN_API_URL = ""
   
   
   def getAllDatasets4PromotionByTenantcode(sc : SparkContext, tenantCode: String) : List[DatasetInfo] = {
      val adminApiUrl: String = ADMIN_API_URL
 	 LOG.info("[[AdminApiDelegate::getAllDatasets4PromotionByTenantcode]] Starting getAllDatasets4PromotionByTenantcode:"+adminApiUrl)
  
 	 val listaDataset = BackofficeListaClient.getListStreamDatasetByTenantCode(adminApiUrl, tenantCode, "getAllDatasets4PromotionByTenantcode").asScala
   
 	 LOG.info("[[AdminApiDelegate::getAllDatasets4PromotionByTenantcode]] listaDataset.size="+listaDataset.size)
  
 	 
 	 //var datasets = listaDataset.filter(_.getDataset.getAvailablespeed.equals(true) ).filterNot(_.getDataset.getDatasetSubtype.getDatasetSubtype().equals("binaryDataset")).filter(_.getStatus().getDescriptionStatus.equals("installed"))
 	 var datasets = listaDataset.filter(_.getDataset.getAvailablespeed.equals(true) )
 	 
 	 LOG.info("[[AdminApiDelegate::getAllDatasets4PromotionByTenantcode]] datasets.size (1)="+datasets.size)
 	 
 	 
 	 datasets=datasets.filterNot(_.getDataset.getDatasetSubtype.getDatasetSubtype().equals("binaryDataset"))
 	 
 	 LOG.info("[[AdminApiDelegate::getAllDatasets4PromotionByTenantcode]] datasets.size (2)="+datasets.size)
 	 
 	 
 	 datasets=datasets.filter(_.getStatus().getDescriptionStatus.equals("installed"))
 	 LOG.info("[[AdminApiDelegate::getAllDatasets4PromotionByTenantcode]] datasets.size (3)="+datasets.size)
 	 
 	 var ret:ListBuffer[DatasetInfo]=new ListBuffer[DatasetInfo]
      
   for (ds <- datasets) {
          ret+=castBackofficeDettaglioStreamDatasetResponseToDatasetInfo(sc, ds)
   }

    LOG.info("[[AdminApiDelegate::getAllDatasets4PromotionByTenantcode]] loaded ")
    return ret.toList    
 	}
   
   def getDatasets4PromotionByIdDataset(sc : SparkContext, idDataset:Integer) : DatasetInfo = {
 	  LOG.info("[[MongoMetadataDelegate::getDatasets4PromotionByIdDataset]] Starting getDatasets4PromotionByIdDataset")
 	 val adminApiUrl: String = ADMIN_API_URL
 	   	 LOG.info("[[AdminApiDelegate::getAllDatasets4PromotionByTenantcode]] Starting getAllDatasets4PromotionByTenantcode:"+adminApiUrl)
 	 LOG.info("[[AdminApiDelegate::getAllDatasets4PromotionByIdDataset]] Starting ADMIN_API_URL:"+ADMIN_API_URL)

 	  
   val ds = BackofficeDettaglioClient.getBackofficeDettaglioStreamDatasetByIdDataset(adminApiUrl, idDataset, false, "getDatasets4PromotionByIdDataset")
   var dsinfo: DatasetInfo = new DatasetInfo()
    
    if ((ds.getDataset.getAvailablespeed.equals(true) ) & (!ds.getDataset.getDatasetSubtype.getDatasetSubtype().equals("binaryDataset")) & (ds.getStatus.getDescriptionStatus().equals("installed")))
       dsinfo  = castBackofficeDettaglioStreamDatasetResponseToDatasetInfo(sc, ds)
    else throw new Exception("invalid dataset '"+ds.getDataset.getIddataset+"' !! Invalid value for:AvailableSpeed or Subtype or Status") 

    LOG.info("[[MongoMetadataDelegate::getDatasets4PromotionByIdDataset]] loaded ")
	  return dsinfo 
	}
   
   def castBackofficeDettaglioStreamDatasetResponseToDatasetInfo(sc : SparkContext, ds: BackofficeDettaglioStreamDatasetResponse) : DatasetInfo = {
	      LOG.info("[[AdminApiDelegate::castBackofficeDettaglioStreamDatasetResponseToDatasetInfo]] ds= "+ds)

	      var curds:DatasetInfo= new DatasetInfo()

		    curds.availableSpeed=ds.getDataset.getAvailablespeed()
			  curds.datasetVersion = ds.getVersion()
			  curds.idDataset = ds.getDataset.getIddataset()
			  curds.datasetCode = ds.getDataset.getDatasetcode()
			  curds.subType = ds.getDataset.getDatasetSubtype.getDatasetSubtype()
			  curds.tenantCode= ds.getTenantManager.getTenantcode()
			  curds.organizationCode= ds.getOrganization.getOrganizationcode()
			  curds.idOrganization = ds.getOrganization.getIdOrganization()
			  curds.dbHiveSchema=ds.getDataset.getDbhiveschema()
			  curds.dbHiveTable=ds.getDataset.getDbhivetable()
			  var fields = ds.getComponents().asScala
        curds.campi=fields.map( field =>
			   new Field(field.getName,field.getDataType.getDatatypecode())
					).toList		
        curds.dataDomain = ds.getDomain.getDomaincode()
        curds.codSubDomain = ds.getSubdomain.getSubdomaincode()
        if (curds.codSubDomain == null || curds.codSubDomain == "") {
          curds.codSubDomain = curds.codSubDomain
        }	     
	      if (curds.subType.equals("streamDataset") || curds.subType.equals("socialDataset")) {
  		  curds.streamCode=ds.getStream.getStreamcode()
  		  curds.vESlug=ds.getStream.getSmartobject.getSlug()
	      }
  		  curds.solrCollection=ds.getDataset.getSolrcollectionname()
  		  curds.jdbcdbhive=ds.getDataset.getJdbcdbname()
  		  curds.jdbctablename=ds.getDataset.getJdbctablename()
	  
	  return curds
	}  
   
   def createOrUpdateAllineamento(idDataset: Integer, datasetVersion: Integer, newObjectID: String, idOrganization: Integer) = {

    var json = new org.json.JSONStringer().`object`()
      .key("idDataset").value(idDataset)
      .key("datasetVersion").value(datasetVersion)
      .key("lastMongoObjectId").value(newObjectID)
      .endObject().toString()
      val adminApiUrl: String = ADMIN_API_URL

    try {
      BackOfficeCreateClient.createOrUpdateAllineamento(
        adminApiUrl, json, idOrganization, "ThreadDatasetDownloadCSV")
    } finally {
      LOG.info("BackOfficeCreateClient.createOrUpdateAllineamento, adminApiUrl=" + adminApiUrl)
      LOG.info("BackOfficeCreateClient.createOrUpdateAllineamento, idOrg=" + idOrganization)
      LOG.info("BackOfficeCreateClient.createOrUpdateAllineamento, json=" + json)
      LOG.info("BackOfficeCreateClient.createOrUpdateAllineamento, lastMongoObjectId=" + newObjectID)
    }
  }
   
   
   def getDatasetsByGroup(sc : SparkContext, idGropu:Integer, groupVersion:Integer, checkForHiveToHivePromotion: Boolean, checkForHiveToSolrPromotion:Boolean) : List[DatasetInfo] = {
 	  LOG.info("[[AdminApiDelegate::getDatasetsByGroup]] Starting getDatasetsForHivePromotionByIdDataset")
 	  
 	  
 	  
 	    if (false == (checkForHiveToHivePromotion || checkForHiveToSolrPromotion) )  throw new Exception("invalid parameters  checkForHiveToHivePromotion || checkForHiveToSolrPromotion is false")  
 
 	 val adminApiUrl: String = ADMIN_API_URL
 	 LOG.info("[[AdminApiDelegate::getDatasetsByGroup]] Starting ADMIN_API_URL:"+adminApiUrl)

 	  
   val listaDataset = BackofficeListaClient.getListStreamDatasetByGroupIdVersion(adminApiUrl, idGropu, groupVersion, "getDatasetsByGroup").asScala
   
   
 	 var datasets = listaDataset.filterNot(_.getDataset.getDatasetSubtype.getDatasetSubtype().equals("binaryDataset")).filter(_.getStatus().getDescriptionStatus.equals("installed"))
   
   if (checkForHiveToSolrPromotion) {
     datasets = datasets.filter(_.getDataset.getAvailablespeed.equals(true) )
   }
   if (checkForHiveToSolrPromotion) {
     datasets = datasets.filter(_.getDataset.getJdbcdburl.equals("yucca_datalake") )
     datasets = datasets.filter(_.getDataset.getJdbcdbtype.equals("HIVE") )
     datasets = datasets.filter(_.getDataset.getIstransformed.equals(true))
     
   } 	 
     
 	 LOG.info("[[AdminApiDelegate::getDatasetsByGroup]] datasets.size (1)="+datasets.size + " listaDataset.size="+listaDataset.size )
 	 
 	 
 	 var ret:ListBuffer[DatasetInfo]=new ListBuffer[DatasetInfo]
      
   for (ds <- datasets) {
          ret+=castBackofficeDettaglioStreamDatasetResponseToDatasetInfo(sc, ds)
   }

    LOG.info("[[AdminApiDelegate::getDatasetsByGroup]] loaded ")
    return ret.toList    
   
   
   
	}
   
   def getDatasetsForHivePromotionByIdDataset(sc : SparkContext, idDataset:Integer) : DatasetInfo = {
 	  LOG.info("[[AdminApiDelegate::getDatasetsForHivePromotionByIdDataset]] Starting getDatasetsForHivePromotionByIdDataset")
 	 val adminApiUrl: String = ADMIN_API_URL
 	 LOG.info("[[AdminApiDelegate::getDatasetsForHivePromotionByIdDataset]] Starting ADMIN_API_URL:"+adminApiUrl)

 	  
   val ds = BackofficeDettaglioClient.getBackofficeDettaglioStreamDatasetByIdDataset(adminApiUrl, idDataset, false, "getDatasetsForHivePromotionByIdDataset")
   var dsinfo: DatasetInfo = new DatasetInfo()
    
    if ((ds.getDataset.getJdbcdburl().equals("yucca_datalake")) & (ds.getDataset.getJdbcdbtype.equals("HIVE")) & (ds.getDataset().getIstransformed.equals(true)))
       dsinfo  = castBackofficeDettaglioStreamDatasetResponseToDatasetInfo(sc, ds)
    else throw new Exception("invalid dataset '"+ds.getDataset.getIddataset+"' !! Invalid value for:JdbcUrl or JdbcType or IsTransformed") 

    LOG.info("[[AdminApiDelegate::getDatasetsForHivePromotionByIdDataset]] loaded ")
	  return dsinfo 
	}
}
