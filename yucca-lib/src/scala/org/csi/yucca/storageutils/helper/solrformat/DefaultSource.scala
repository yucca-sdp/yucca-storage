package org.csi.yucca.storageutils.helper.solrformat


import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.sources.CreatableRelationProvider
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.RelationProvider

import com.lucidworks.spark.SolrRelation


class DefaultSource extends RelationProvider with CreatableRelationProvider with DataSourceRegister {

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    try {
      new YuccaSolrRelation(parameters, sqlContext, None)
    } catch {
      case re: RuntimeException => throw re
      case e: Exception => throw new RuntimeException(e)
    }
  }

  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      df: DataFrame): BaseRelation = {
    try {
      // TODO: What to do with the saveMode?
      
      System.out.println("HiveToSolrSingleDataset:: *************************************")
      
      val solrRelation: YuccaSolrRelation = new YuccaSolrRelation(parameters, sqlContext, Some(df))
      solrRelation.insert(df, overwrite = true)
      solrRelation
    } catch {
      case re: RuntimeException => throw re
      case e: Exception => throw new RuntimeException(e)
    }
  }

  override def shortName(): String = "solr_csi"

  
}