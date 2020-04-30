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

object MetadataGovDashb_InfoTables {

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
      //Scrive sulle tabelle
      //yucca_metadata.solr_tot_bytes --> contiene occupazione spazio in bytes e nome della collezione solr per ogni tenant
      //yucca_metadata.area_stage_bytes --> contiene occupazione spazio in bytes dell'area di stage per ogni tenant (db e files)
      
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
      LOG.info("[[MetadataGovDashb_InfoTables::main]] timestamp --> " + timestamp)

      val conf = new SparkConf().set("spark.yarn.queue", valueQueue).setExecutorEnv(Array(("java.security.auth.login.config", "jaas-client.conf")));
      val sparkContext = new SparkContext(conf)
      val sqlContextHive = new org.apache.spark.sql.hive.HiveContext(sparkContext)

      import sqlContextHive.implicits._
      
      val ts = lit(timestamp).cast("timestamp")
      
      //Creo tabelle INFO su yucca_metadata
      //Tabella che legge il file CSV con le informazioni aggiunte manualmente
      //Utilizzo tabella external HIVE che legge i dati caricati da user portal Yucca: stg_smartlab_sdp_governoyucca.ext_registrofruizionemanuale_6609
      /*
      val registro_offline = sqlContextHive.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("delimiter",";").load("/CSI/yucca/rawdata/files/metadati_tenant_agg.csv").alias("b")
      registro_offline.write.mode("overwrite").format("orc").saveAsTable("yucca_metadata.info_registro_offline")
      sqlContextHive.sql("analyze table yucca_metadata.info_registro_offline compute statistics") 
      */
      
      //Tabella info_tenant
      val info_tenant = sqlContextHive.sql("""select idecosystem, tenanttype,organizationcode,tenantname,meta_tenant.tenantcode,
      concat(userlastname,' ',userfirstname)  as referente, useremail as email_referente,
      if(max(lasttimeid) <= '1970-01-01 01:00:00.0', null, cast(to_date(max(lasttimeid)) as string)) as dataultimoaggiornamento,
      case zeppelin when 'undefined' then '' else zeppelin end zeppelin,
      case hasstage when 'undefined' then '' else hasstage end stage,
      case dataattivazione when 'undefined' then '' else dataattivazione end dataattivazione,
      case datadisattivazione when 'undefined' then '' else datadisattivazione end datadisattivazione
      from yucca_metadata.meta_tenant  
          left outer join yucca_metadata.meta_dataset on (meta_tenant.tenantcode = meta_dataset.tenantcode) 
          left outer join yucca_metadata.solr_dim_dataset on 
              (meta_dataset.iddataset = solr_dim_dataset.iddataset and meta_dataset.datasetversion = solr_dim_dataset.datasetversion ) 
      group by idecosystem, tenanttype,organizationcode,codDeploymentStatus,tenantname,meta_tenant.tenantcode,
          userlastname,userfirstname, useremail,
          zeppelin,
          hasstage,
          dataattivazione,
          datadisattivazione
      order by meta_tenant.tenantcode""")
      //non necessario scrivere su Hive
      //info_tenant.write.mode("overwrite").format("orc").saveAsTable("yucca_metadata.info_tenant")
      //sqlContextHive.sql("analyze table yucca_metadata.info_tenant compute statistics")
      
      //Calcolo dimensioni delle tabelle Hive, a partire dalle info del metastore di Hive
      //Aggiungo flag per indicare se per la tabella è presente il metadato dbHiveTable (flag_approx=0).
      //Se non è presente (flag_approx=1), attribuiamo la tabella ad una coppia tenant/dbHive
      //andando a leggere la tab statica yucca_metadata.tenant_hive
      //Non consideriamo le tabelle db_catalog --
      val df_dim_hive_table_tenant = sqlContextHive.sql("""
      select 
      tenant_hive.tenantcode as tenantcode, 
      db_catalog_global.db_name dbname, 
      db_catalog_global.tbl_name, 
      db_catalog_global.total_size, 
      db_catalog_global.num_rows,
      '1' as flag_approx
       from 
      yucca_metadata.db_catalog_global
          left outer join
              yucca_metadata.tenant_hive on db_catalog_global.db_name=tenant_hive.db_hive
          left outer join  
              (select allds.* from 
               (select iddataset, max(datasetversion) as datasetversion from yucca_metadata.meta_dataset_extended group by iddataset) maxds
               inner join yucca_metadata.meta_dataset_extended allds  on (maxds.iddataset=allds.iddataset and maxds.datasetversion=allds.datasetversion and allds.datasetstatus='inst' )) lastds
              	on upper(db_catalog_global.tbl_name) = upper(lastds.dbhivetable) and
              	upper(db_catalog_global.db_name) = upper(lastds.dbhiveschema) 
      where ( db_catalog_global.db_name like 'db_%' or db_catalog_global.db_name like 'transf_%' ) and db_catalog_global.db_name not in ('db_csi_log', 'db_celi') and (lastds.dbhiveschema is null and lastds.dbhivetable is null)
      and tbl_name not like 'db_catalog%'
      union all
      select 
      lastds.tenantcode as tenantcode,
      db_catalog_global.db_name dbname, 
      db_catalog_global.tbl_name, 
      db_catalog_global.total_size, 
      db_catalog_global.num_rows,
      '0' as flag_approx
      	   from 
      yucca_metadata.db_catalog_global 
      left outer join  
      (select allds.* from 
       (select iddataset, max(datasetversion) as datasetversion from yucca_metadata.meta_dataset_extended group by iddataset) maxds
       inner join yucca_metadata.meta_dataset_extended allds  on (maxds.iddataset=allds.iddataset and maxds.datasetversion=allds.datasetversion and allds.datasetstatus='inst' )) lastds
      	on upper(db_catalog_global.tbl_name) = upper(lastds.dbhivetable) and
      	upper(db_catalog_global.db_name) = upper(lastds.dbhiveschema) 
      where (db_catalog_global.db_name like 'db_%' or db_catalog_global.db_name like 'transf_%' ) and db_catalog_global.db_name not in ('db_csi_log', 'db_celi')  and (lastds.dbhiveschema is not null or lastds.dbhivetable is not null)           
      and tbl_name not like 'db_catalog%' 
      """)
      
      df_dim_hive_table_tenant.write.mode("overwrite").format("orc").saveAsTable("yucca_metadata.info_dim_hive_table_tenant")
      sqlContextHive.sql("analyze table yucca_metadata.info_dim_hive_table_tenant compute statistics")
      
      val df_dim_hive = sqlContextHive.sql("""
      select meta_tenant.tenantcode as tenant,
      case when max(jj.total_size) is not null then ROUND(max(jj.total_size)/(1024*1024),1) else '0.0' end as hive_dim_MB
      from yucca_metadata.meta_tenant
      	left outer join (select tenantcode, sum(total_size) as total_size from yucca_metadata.info_dim_hive_table_tenant group by tenantcode) as jj  on (jj.tenantcode = meta_tenant.tenantcode)
      group by meta_tenant.tenantcode
      order by meta_tenant.tenantcode
      """)
      //Non necessario scrivere su Hive
      //df_dim_hive.write.mode("overwrite").format("orc").saveAsTable("yucca_metadata.info_dim_hive")
      //sqlContextHive.sql("analyze table yucca_metadata.info_dim_hive compute statistics")
     
      val df_dim_csv = sqlContextHive.sql("""
      select meta_tenant.tenantcode as tenant,
      case 
      when sum(dim_dataset.num_bytes) is not null then ROUND(sum(dim_dataset.num_bytes)/(1024*1024),1) 
      else '0.0' 
      end 
      as hdfs_csv_dim_MB
      from yucca_metadata.meta_tenant
          left outer join yucca_metadata.meta_dataset on (meta_tenant.tenantcode = meta_dataset.tenantcode) 
          left outer join yucca_metadata.dim_dataset on (meta_dataset.datasetcode = dim_dataset.datasetcode AND meta_dataset.datasetversion = dim_dataset.datasetversion)
      group by meta_tenant.tenantcode
      order by meta_tenant.tenantcode 
      """)
      //Non necessario scrivere su Hive
      //df_dim_csv.write.mode("overwrite").format("orc").saveAsTable("yucca_metadata.info_dim_csv")
      //sqlContextHive.sql("analyze table yucca_metadata.info_dim_csv compute statistics")

      val df_dim_binary = sqlContextHive.sql("""
      select meta_tenant.tenantcode as tenant, case when tb_j.bin_dim_MB is not null then tb_j.bin_dim_MB else '0.0' end as bin_dim_MB from
      yucca_metadata.meta_tenant
      left outer join 
      (select tenantcode,case when sum(dim_dataset.num_bytes) is not null then ROUND(sum(dim_dataset.num_bytes)/(1024*1024),1) else '0.0' end as bin_dim_MB  
      from (select tenantcode, binaryiddataset, binarydatasetversion from yucca_metadata.meta_dataset group by tenantcode, binaryiddataset, binarydatasetversion) meta_binary
      join yucca_metadata.dim_dataset 
      where (dim_dataset.datasetcode like concat('%_',meta_binary.binaryiddataset) AND meta_binary.binarydatasetversion = dim_dataset.datasetversion) 
      group by tenantcode) tb_j
      on (meta_tenant.tenantcode = tb_j.tenantcode)
      order by tenant
      """)
      //Non necessario scrivere su Hive
      //df_dim_binary.write.mode("overwrite").format("orc").saveAsTable("yucca_metadata.info_dim_binary")
      //sqlContextHive.sql("analyze table yucca_metadata.info_dim_binary compute statistics")
      
      val df_dim_solr = sqlContextHive.sql("""
      select tenantcode as tenant, round((sum(solrtenantcollsize)/(1024*1024) ),1) solrtenantcollmb
      from (	
      select 	solrtenantcollnumrecord.tenantcode, solrtenantcollnumrecord.solrcollectionname, 
      		coalesce(solr_byte,0) solrtotbyte , 
      		coalesce(tenantcolnumrec,0) solrtenantcollnumrecord,
      		coalesce(totcolnumrec,0) solrtotcollnumrecord,
      		case 
      			when totcolnumrec is not null then 100 * (coalesce(tenantcolnumrec,0)/coalesce(totcolnumrec,0)) 
      			else 0
      		end solrtenantcollperc,
      		case 
      			when totcolnumrec is not null then coalesce(solr_byte,0) * (coalesce(tenantcolnumrec,0)/coalesce(totcolnumrec,0))  
      			else 0
      		end solrtenantcollsize
      from 
      ( select sum(solr_dim_dataset.numrecord) tenantcolnumrec, meta_tenant.tenantcode, meta_dataset_extended.solrcollectionname
      			from yucca_metadata.meta_tenant 
      			    left outer join yucca_metadata.meta_dataset_extended on (meta_tenant.tenantcode= meta_dataset_extended.tenantcode) 
      			    left outer join yucca_metadata.solr_dim_dataset on (meta_dataset_extended.iddataset= solr_dim_dataset.iddataset AND meta_dataset_extended.datasetversion= solr_dim_dataset.datasetversion)
      			group by meta_tenant.tenantcode, meta_dataset_extended.solrcollectionname) solrtenantcollnumrecord
       left join yucca_metadata.solr_tot_bytes solrtotbyte
       on (solrtenantcollnumrecord.tenantcode = solrtotbyte.tenantcode and solrtenantcollnumrecord.solrcollectionname = solrtotbyte.solrcollectionname  )
       left join 
       (select sum(solr_dim_dataset.numrecord) totcolnumrec, meta_dataset_extended.solrcollectionname
      			from yucca_metadata.meta_tenant 
      			    left outer join yucca_metadata.meta_dataset_extended on (meta_tenant.tenantcode= meta_dataset_extended.tenantcode) 
      			    left outer join yucca_metadata.solr_dim_dataset on (meta_dataset_extended.iddataset= solr_dim_dataset.iddataset AND meta_dataset_extended.datasetversion= solr_dim_dataset.datasetversion)
      			group by meta_dataset_extended.solrcollectionname) solrtotcolnumrecord
       on (solrtenantcollnumrecord.solrcollectionname = solrtotcolnumrecord.solrcollectionname )
      --order by solrtenantcollnumrecord.solrcollectionname
      )  a group by a.tenantcode
      """)
      //Non necessario scrivere su Hive
      //df_dim_solr.write.mode("overwrite").format("orc").saveAsTable("yucca_metadata.info_dim_solr")
      //sqlContextHive.sql("analyze table yucca_metadata.info_dim_solr compute statistics")
      
      val df_dim_stg = sqlContextHive.sql("""
      select meta_tenant.tenantcode as tenant,
      case 
      when sum(byte) is not null then ROUND(sum(byte)/(1024*1024),1) 
      else '0.0' 
      end as hdfs_stage_dim
      from yucca_metadata.meta_tenant
      left join
      	yucca_metadata.area_stage_bytes on (area_stage_bytes.tenantcode==meta_tenant.tenantcode)
      group by meta_tenant.tenantcode
      order by meta_tenant.tenantcode
      """)
      //Non necessario scrivere su Hive
      //df_dim_stg.write.mode("overwrite").format("orc").saveAsTable("yucca_metadata.info_dim_stg")
      //sqlContextHive.sql("analyze table yucca_metadata.info_dim_stg compute statistics")
      
      val df_num_so_stream = sqlContextHive.sql("""
      select meta_tenant.tenantcode as tenant,count(DISTINCT idvirtualentity) as num_smartobjects,count(DISTINCT idstream) as num_streams,
      (count(DISTINCT idvirtualentity)+count(DISTINCT idstream)) as tot_so_stream
      from yucca_metadata.meta_tenant
      left outer join  yucca_metadata.meta_stream  ON (meta_tenant.tenantcode = meta_stream.tenantcode) 
      group by meta_tenant.tenantcode
      """)
      //Non necessario scrivere su Hive
      //df_num_so_stream.write.mode("overwrite").format("orc").saveAsTable("yucca_metadata.info_num_so_stream")
      //sqlContextHive.sql("analyze table yucca_metadata.info_num_so_stream compute statistics")
      
      //Tabella info_tenant_completo
      val dfDimTotTenant = df_dim_hive.join(df_dim_csv,Seq("tenant"),"left_outer").join(df_dim_binary,Seq("tenant"),"left_outer").join(df_dim_solr,Seq("tenant"),"left_outer").join(df_dim_stg,Seq("tenant"),"left_outer").join(df_num_so_stream,Seq("tenant"),"left_outer")
      val dfSommaDim = dfDimTotTenant.withColumn("dimTotale",df_dim_hive("hive_dim_MB")+df_dim_csv("hdfs_csv_dim_MB")+df_dim_binary("bin_dim_MB")+df_dim_solr("solrtenantcollmb")+df_dim_stg("hdfs_stage_dim"))
      val info_tenant_completo = info_tenant.join(dfSommaDim, info_tenant("tenantcode") === dfSommaDim("tenant"),"left_outer")
      info_tenant_completo.write.mode("overwrite").format("orc").saveAsTable("yucca_metadata.info_tenant_completo")
      sqlContextHive.sql("analyze table yucca_metadata.info_tenant_completo compute statistics")
      
      //Tabella per report "Numero chiamate mensili e media al sec "
      //qui 'tenant' è tenantcode
      var dfChiamate=sqlContextHive.sql("""
      SELECT tenant as tenantcode, year as anno, month as mese, sum(num_chiamate_mese) as num_chiamate_mese, max(fruitori_distinti_tenant) as fruitori_distinti,
      sum(num_chiamate_mese)/(30*24*60*60) as media_al_sec, max(ultima_chiamata) as ultima_chiamata
      FROM db_csi_log.sdnet_odata_groupby_partmonth 
      group by tenant, year ,month
      """)

      var dfC_1=dfChiamate.groupBy($"tenantcode",$"anno").agg(count($"mese"),sum($"num_chiamate_mese"),(sum($"num_chiamate_mese")/count($"mese")).as("media_chiamate_anno") )
      var dfMediaChiamateAnno=dfC_1.join(info_tenant,Seq("tenantcode")).select("tenantcode","anno","media_chiamate_anno","datadisattivazione")
      var dfChiamateTot=dfChiamate.join(dfMediaChiamateAnno,Seq("tenantcode","anno"))
      dfChiamateTot.write.mode("overwrite").format("orc").saveAsTable("yucca_metadata.info_chiamate_mese")
      sqlContextHive.sql("analyze table yucca_metadata.info_chiamate_mese compute statistics")

      //Tabella per report "Elenco dataset Yucca"
      var dfElencoDataset=sqlContextHive.sql("""
      select meta_tenant.organizationcode as organizzazione, meta_tenant.tenantname, meta_tenant.tenantcode,
      meta_tenant.datadisattivazione,
      meta_dataset.iddataset as codice_dataset, 
      meta_dataset.datasetstatus as status,
      meta_dataset.datasetcode as nome_dataset, 
      regexp_replace(meta_dataset.descrds,'\n',' ') as descr_dataset,
      meta_dataset.visibility as visibilita, 
      meta_dataset.datadomain as dominio, meta_dataset.codsubdomain as sottodominio, meta_dataset.elencotag as tag
      from yucca_metadata.meta_tenant 
      join yucca_metadata.meta_dataset ON (meta_tenant.tenantcode = meta_dataset.tenantcode)
      join (select iddataset, max(datasetversion) as datasetversion from yucca_metadata.meta_dataset group by iddataset) last_ds 
      on (meta_dataset.iddataset=last_ds.iddataset AND meta_dataset.datasetversion=last_ds.datasetversion)
      order by organizzazione, tenantname, codice_dataset
      """)
      dfElencoDataset.write.mode("overwrite").format("orc").saveAsTable("yucca_metadata.info_elenco_dataset")
      sqlContextHive.sql("analyze table yucca_metadata.info_elenco_dataset compute statistics")

      
      //Tabella info_registro_fruizione: stesse colonne della info_registro_offline, ma i valori sono calcolati e non inseriti manualmente
      //Inserisco anche informazioni: chiamate mese e numero dataset
      //val registro_output_completo= info_tenant_completo.alias("t").join(registro_offline, $"t.tenantname"===$"b.tenantname","outer")
      val registro_offline=sqlContextHive.sql("select * from stg_smartlab_sdp_governoyucca.ext_registrofruizionemanuale_6609").alias("b")

      val dataset_tenant=sqlContextHive.sql("select tenantcode, count(codice_dataset) as Dataset from yucca_metadata.info_elenco_dataset group by tenantcode").alias("d")
      val chiamateAnnoCorr = sqlContextHive.sql("select distinct tenantcode, media_chiamate_anno as oData from yucca_metadata.info_chiamate_mese where anno=year(current_date)").alias("ca") 
      val registro_fruizione= info_tenant_completo.alias("t").join(dataset_tenant,Seq("tenantcode"),"left_outer").join(chiamateAnnoCorr,Seq("tenantcode"),"left_outer").join(registro_offline, Seq("tenantcode"),"left_outer")
      
      //registro_fruizione.select("b.idecosystem","t.tenantname", "t.tenantcode", "t.tenanttype","t.referente", "t.email_referente", "t.dataultimoaggiornamento","t.dataattivazione","t.datadisattivazione","b.in_consuntivo", "b.pagante", "t.dimTotale", "ca.oData", "t.tot_so_stream", "b.modulo_data_prep", "b.modulo_data_in_out", "d.Dataset", "b.note").write.mode("overwrite").format("orc").saveAsTable("yucca_metadata.info_registro_fruizione")
      registro_fruizione.selectExpr("b.idecosystem","b.ecosistema","b.ente_pagante","t.organizationcode as ente_titolare","b.incaricato_trattamento", "b.progetto", "t.tenantname", "t.tenantcode", "t.tenanttype","t.referente", "t.email_referente", "t.dataultimoaggiornamento","t.dataattivazione","t.datadisattivazione","b.in_consuntivo", "b.pagante", "t.dimTotale", "ca.oData", "t.num_smartobjects","t.num_streams","t.tot_so_stream", "b.modulo_data_prep", "b.modulo_data_in_out", "d.Dataset", "b.note").write.mode("overwrite").format("orc").saveAsTable("yucca_metadata.info_registro_fruizione")
      sqlContextHive.sql("analyze table yucca_metadata.info_registro_fruizione compute statistics")
      
      
      //Tabella per report "Dimensione CSV su HDFS e numero record dei tenant"
      var dfElenco_all = sqlContextHive.sql("""
      select meta_tenant.tenantname , meta_tenant.tenantcode,
        meta_tenant.datadisattivazione as datadisattivazione,
        count(distinct meta_dataset.iddataset) as dataset,
        case when sum(dim_dataset.num_rows) is not null then sum(dim_dataset.num_rows) else 0 end as num_records_hdfs,
        case when max(lasttimeid) is not null then max(lasttimeid) else null end as last_update_speedlayer
        from yucca_metadata.meta_tenant
            left outer join yucca_metadata.meta_dataset on (meta_tenant.tenantcode = meta_dataset.tenantcode) 
            left outer join yucca_metadata.dim_dataset on (meta_dataset.datasetcode = dim_dataset.datasetcode AND meta_dataset.datasetversion = dim_dataset.datasetversion)
            left outer join yucca_metadata.solr_dim_dataset on (meta_dataset.iddataset = solr_dim_dataset.iddataset AND meta_dataset.datasetversion = solr_dim_dataset.datasetversion)
        group by meta_tenant.tenantname, meta_tenant.tenantcode, meta_tenant.datadisattivazione
        """)
        var dfElenco_multi = sqlContextHive.sql("""
          select tenantname as multi_ds_tenant, tenantcode, count(codice_dataset) as dataset_multi from yucca_metadata.info_elenco_dataset where dominio='MULTI' group by tenantcode, tenantname 
          """)
        var dfElencoDimCSVNumDataset=dfElenco_all.join(dfElenco_multi,Seq("tenantcode"),"left_outer").orderBy("tenantname")
        dfElencoDimCSVNumDataset.select("tenantname","datadisattivazione","dataset","dataset_multi","num_records_hdfs","last_update_speedlayer").orderBy("tenantname").write.mode("overwrite").format("orc").saveAsTable("yucca_metadata.info_dim_dataset_tenant")
        sqlContextHive.sql("analyze table yucca_metadata.info_dim_dataset_tenant compute statistics")
      
    } catch {
      case e: Exception =>
        LOG.error("[[MetadataGovDashb_InfoTables::main]]" + e, e)
        throw e
    } finally {
      LOG.info("[[MetadataGovDashb_InfoTables::main]] END")

    }

    System.exit(retCode)

  }


}