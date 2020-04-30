package org.csi.yucca.metadataalignment
import org.slf4j.LoggerFactory
import collection.immutable
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.hive._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import scala.collection.immutable
import scala.collection.mutable.Buffer
import org.joda.time.DateTime
import scopt._
import org.apache.spark.sql.DataFrame

object MetadataGovDashb_ExportInfo {

  val LOG = LoggerFactory.getLogger(getClass)
  val zkHost: String = ""

  case class CliOptions(
      timestamp: String = "",
      queue: String = "") {
  }

  val parser = new scopt.OptionParser[CliOptions]("ingest") {
    head("Start ", "0.x")

    opt[String]("timestamp").required().action((timestamp, c) => { c.copy(timestamp = timestamp) })
      .text("Timestamp in form 2018-10-14 11:41:26 ")
     opt[String]("queue").optional().action((queue, c) => { c.copy(queue = queue) })
      .text("Queue (optional, default=produzione)")
  }
  
 
def main(args: Array[String]) = {
    
    import sys.process._
        
    var retCode: Integer = -1
    try {
      System.setProperty("java.security.auth.login.config", "jaas-client_sdpbatch.conf");
      System.setProperty("spark.driver.allowMultipleContexts", "true");

      LOG.info("[[MetadataGovDashb_InfoTables::main]] BEGIN")
      
      var timestamp: String = ""
      var queue:Option[String] = None
      parser.parse(args, CliOptions()) match {
        case None =>
        // syntax error on command line
        case Some(opts) => {
          timestamp = opts.timestamp
          queue = Some(opts.queue)
        }
      }

      val valueQueue: String = queue match {
        case None => "produzione"
        case Some(s: String) => s
      }
      
      LOG.info("[[MetadataGovDashb_ExportInfo::main]] timestamp --> " + timestamp)
      LOG.info("[[MetadataGovDashb_ExportInfo::main]] queue --> " + valueQueue)
      
      val conf = new SparkConf().set("spark.yarn.queue", valueQueue).setExecutorEnv(Array(("java.security.auth.login.config", "jaas-client.conf")));
      val sparkContext = new SparkContext(conf)
      val sqlContextHive = new org.apache.spark.sql.hive.HiveContext(sparkContext)

      import sqlContextHive.implicits._
      import java.util.Date
      import java.text.SimpleDateFormat
      import java.util.Calendar 

      val ts = lit(timestamp).cast("timestamp")

      val dirExport="/CSI/yucca/rawdata/files"
      val oggi = Calendar.getInstance().getTime()
      val format_oggi = new SimpleDateFormat("yyyyMMdd_HHmmss")
      val dataoggi = format_oggi.format(oggi)
      var nomeFileExp=""
      
      //Tabella external con dati caricati da UserPortal Yucca, in join con la tabella yucca_metadata.info_registro_fruizione aggiornata ogni giorno
      val registro_offline=sqlContextHive.sql("select * from stg_smartlab_sdp_governoyucca.ext_registrofruizionemanuale_6609").alias("b")
      val info_tenant_completo = sqlContextHive.sql("select * from yucca_metadata.info_tenant_completo")
      val registro_fruizione = sqlContextHive.sql("select * from yucca_metadata.info_registro_fruizione").alias("t")
      val registro_fruizione_exp= registro_fruizione.join(registro_offline, Seq("tenantcode"),"left_outer")
      val info_chiamate = sqlContextHive.sql("select * from yucca_metadata.info_chiamate_mese").drop("datadisattivazione").alias("p")
      val registroChiamate = info_chiamate.join(registro_fruizione, Seq("tenantcode"),"left_outer") 
      
      //report registro di fruizione
      nomeFileExp = s"$dirExport/registro_di_fruizione_$dataoggi"
      val dfRegistroExp=registro_fruizione_exp.selectExpr("b.idecosystem", "b.ecosistema", "b.ente_pagante", "t.ente_titolare", "b.incaricato_trattamento", "b.progetto", "t.tenantname", "t.tenantcode", "t.tenanttype", "t.referente", "t.email_referente", "t.dataultimoaggiornamento", "t.dataattivazione", "t.datadisattivazione", "b.in_consuntivo", "b.pagante", "regexp_replace(cast(t.dimTotale as string),'\\.',',') as dimTotale", "regexp_replace(t.odata,'\\.',',') as odata", "t.num_smartobjects","t.num_streams", "t.tot_so_stream", "b.modulo_data_prep", "b.modulo_data_in_out", "t.dataset", "b.note").orderBy("t.tenantcode")
      dfRegistroExp.coalesce(1).write.format("com.databricks.spark.csv").option("delimiter",";").mode("overwrite").option("header","true").option("quoteMode","NON_NUMERIC").option("nullValue","").save(nomeFileExp)
      
      //report dimensioni tenant
      nomeFileExp = s"$dirExport/dimensioni_tenant_$dataoggi"
      val dfDimensioniTenantExp = info_tenant_completo.orderBy("tenantcode").selectExpr("tenantcode","regexp_replace(hive_dim_MB,'\\.',',') as hive_dim_MB", "regexp_replace(hdfs_csv_dim_MB,'\\.',',') as hdfs_csv_dim_MB","regexp_replace(bin_dim_MB,'\\.',',') as bin_dim_MB","regexp_replace(cast(solrtenantcollmb as string),'\\.',',') as solrtenantcollmb","regexp_replace(hdfs_stage_dim,'\\.',',') as hdfs_stage_dim","num_smartobjects","num_streams","tot_so_stream", "regexp_replace(cast(dimTotale as string),'\\.',',') as dimTotale")
      dfDimensioniTenantExp.coalesce(1).write.format("com.databricks.spark.csv").option("delimiter",";").mode("overwrite").option("header","true").option("quoteMode","NON_NUMERIC").option("nullValue","").save(nomeFileExp)
      
      //report chiamate
      nomeFileExp = s"$dirExport/report_chiamate_$dataoggi"
      val dfChiamate = registroChiamate.filter(s"tenantname is not null AND  tenantname !=''").selectExpr("idecosystem","tenantname", "anno", "mese", "num_chiamate_mese", "regexp_replace(cast(media_al_sec as string),'\\.',',') as media_al_sec", "fruitori_distinti", "ultima_chiamata", "in_consuntivo", "pagante","media_al_sec as double_media_sec").orderBy(desc("double_media_sec")).drop("double_media_sec")
      dfChiamate.coalesce(1).write.format("com.databricks.spark.csv").option("delimiter",";").mode("overwrite").option("header","true").option("quoteMode","NON_NUMERIC").option("nullValue","").save(nomeFileExp)
      
      //Elenco dataset
      nomeFileExp = s"$dirExport/report_elenco_dataset_$dataoggi"
      val dfElencoDataset = sqlContextHive.sql("select * from yucca_metadata.info_elenco_dataset")
      dfElencoDataset.coalesce(1).write.format("com.databricks.spark.csv").option("delimiter",";").mode("overwrite").option("header","true").option("quoteMode","NON_NUMERIC").option("nullValue","").save(nomeFileExp)

      //Dimensione CSV su HDFS e numero dataset dei tenant
      nomeFileExp = s"$dirExport/report_dim_dataset_tenant_$dataoggi"
      val dfDimensioniDatasetTenant = sqlContextHive.sql("select * from yucca_metadata.info_dim_dataset_tenant")
      dfDimensioniDatasetTenant.coalesce(1).write.format("com.databricks.spark.csv").option("delimiter",";").mode("overwrite").option("header","true").option("quoteMode","NON_NUMERIC").option("nullValue","").save(nomeFileExp)
      
    } catch {
      case e: Exception =>
        LOG.error("[[MetadataGovDashb_ExportInfo::main]]" + e, e)
        throw e
    } finally {
      LOG.info("[[MetadataGovDashb_ExportInfo::main]] END")

    }

    System.exit(retCode)

  }


}