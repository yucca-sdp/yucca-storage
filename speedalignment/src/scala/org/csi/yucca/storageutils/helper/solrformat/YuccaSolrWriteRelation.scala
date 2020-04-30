package org.csi.yucca.storageutils.helper.solrformat

import org.apache.spark.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.InsertableRelation
import org.apache.spark.sql.sources.PrunedFilteredScan
import org.apache.spark.sql.sources.TableScan

import com.lucidworks.spark.SolrConf
import com.lucidworks.spark.SolrRelation

import java.util.UUID

import com.lucidworks.spark.util._
import com.lucidworks.spark.rdd.SolrRDD
import org.apache.http.entity.StringEntity
import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.client.solrj.request.schema.SchemaRequest.{Update, AddField, MultiUpdate}
import org.apache.solr.common.SolrException.ErrorCode
import org.apache.solr.common.{SolrException, SolrInputDocument}
import org.apache.solr.common.params.{CommonParams, ModifiableSolrParams}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.Logging

import scala.collection.{mutable, JavaConversions}
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._
import scala.reflect.runtime.universe._

import com.lucidworks.spark.util.QueryConstants._
import com.lucidworks.spark.util.ConfigurationConstants._

class YuccaSolrWriteRelation 
(
    override val parameters: Map[String, String],
    override val sqlContext: SQLContext,
    override val dataFrame: Option[DataFrame])
  extends SolrRelation  (
    parameters,    sqlContext,    dataFrame){
  
  override def insert(df: DataFrame, overwrite: Boolean): Unit = {

    val zkHost = conf.getZkHost.get
    val collectionId = conf.getCollection.get
    val dfSchema = df.schema
    val solrBaseUrl = SolrSupport.getSolrBaseUrl(zkHost)
    //val solrFields : Map[String, SolrFieldMeta] = SolrQuerySupport.getFieldTypes(Set(), solrBaseUrl, collectionId)
/*
    // build up a list of updates to send to the Solr Schema API
    val fieldsToAddToSolr = new ListBuffer[Update]()
    dfSchema.fields.foreach(f => {
      // TODO: we should load all dynamic field extensions from Solr for making a decision here
      if (!solrFields.contains(f.name) && !f.name.endsWith("_txt") && !f.name.endsWith("_txt_en"))
        fieldsToAddToSolr += new AddField(toAddFieldMap(f).asJava)
    })

    val cloudClient = SolrSupport.getCachedCloudClient(zkHost)
    val solrParams = new ModifiableSolrParams()
    solrParams.add("updateTimeoutSecs","30")
    val addFieldsUpdateRequest = new MultiUpdate(fieldsToAddToSolr.asJava, solrParams)

    logInfo(s"Sending request to Solr schema API to add ${fieldsToAddToSolr.size} fields.")

    val updateResponse : org.apache.solr.client.solrj.response.schema.SchemaResponse.UpdateResponse =
      addFieldsUpdateRequest.process(cloudClient, collectionId)
    if (updateResponse.getStatus >= 400) {
      val errMsg = "Schema update request failed due to: "+updateResponse
      logError(errMsg)
      throw new SolrException(ErrorCode.getErrorCode(updateResponse.getStatus), errMsg)
    }
*/

    val cloudClient = SolrSupport.getCachedCloudClient(zkHost)
    val solrParams = new ModifiableSolrParams()
    solrParams.add("updateTimeoutSecs","30")
      
      
      logInfo("softAutoCommitSecs? "+conf.softAutoCommitSecs)
    if (conf.softAutoCommitSecs.isDefined) {
      val softAutoCommitSecs = conf.softAutoCommitSecs.get
      val softAutoCommitMs = softAutoCommitSecs * 1000
      var configApi = solrBaseUrl
      if (!configApi.endsWith("/")) {
        configApi += "/"
      }
      configApi += collectionId+"/config"

      val postRequest = new org.apache.http.client.methods.HttpPost(configApi)
      val configJson = "{\"set-property\":{\"updateHandler.autoSoftCommit.maxTime\":\""+softAutoCommitMs+"\"}}";
      postRequest.setEntity(new StringEntity(configJson))
      logInfo("POSTing: "+configJson+" to "+configApi)
      SolrJsonSupport.doJsonRequest(cloudClient.getLbClient.getHttpClient, configApi, postRequest)
    }

    val batchSize: Int = if (conf.batchSize.isDefined) conf.batchSize.get else 500
    val generateUniqKey: Boolean = conf.genUniqKey.getOrElse(false)
    val uniqueKey = SolrQuerySupport.getUniqueKey(conf.getZkHost.get, conf.getCollection.get)
    // Convert RDD of rows in to SolrInputDocuments
    val docs = df.rdd.map(row => {
      val schema: StructType = row.schema
      val doc = new SolrInputDocument
      schema.fields.foreach(field => {
        val fname = field.name
        breakable {
          if (fname.equals("_version")) break()
        }
        val fieldIndex = row.fieldIndex(fname)
        val fieldValue : Option[Any] = if (row.isNullAt(fieldIndex)) None else Some(row.get(fieldIndex))
        if (fieldValue.isDefined) {
          val value = fieldValue.get
          value match {
            //TODO: Do we need to check explicitly for ArrayBuffer and WrappedArray
            case v: Iterable[Any] =>
              val it = v.iterator
              while (it.hasNext) doc.addField(fname, it.next())
            case bd: java.math.BigDecimal =>
              doc.setField(fname, bd.doubleValue())
            case _ => doc.setField(fname, value)
          }
        }
      })
      // Generate unique key if the document doesn't have one
      if (generateUniqKey) {
        if (!doc.containsKey(uniqueKey)) {
          doc.setField(uniqueKey, UUID.randomUUID().toString)
        }
      }
      doc
    })
    SolrSupport.indexDocs(solrRDD.zkHost, solrRDD.collection, batchSize, docs)
  }
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
}