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

object MetadataPgToHive {

  val LOG = LoggerFactory.getLogger(getClass)
 
  case class CliOptions(
      pg_url: String = "",
      pg_username: String = "",
      pg_schema: String = "",
      pg_password: String = "",
      timestamp: String = "",
      queue: String = "") {

  }

  val parser = new scopt.OptionParser[CliOptions]("ingest") {
    head("Metadata To Hive Cli ", "0.x")

    opt[String]("pg_url").required().action((pg_url, c) => { c.copy(pg_url = pg_url) })
      .text("Url PostgreSQL sorgente (eg: jdbc:postgresql://server/SDPCONFIG)")
    opt[String]("pg_username").required().action((pg_username, c) => { c.copy(pg_username = pg_username) })
      .text("PostgreSQL Username")
    opt[String]("pg_schema").required().action((pg_schema, c) => { c.copy(pg_schema = pg_schema) })
      .text("PostgreSQL Schema")
    opt[String]("pg_password").required().action((pg_password, c) => { c.copy(pg_password = pg_password) })
      .text("PostgreSQL password")
    opt[String]("timestamp").required().action((timestamp, c) => { c.copy(timestamp = timestamp) })
      .text("Timestamp in form 2018-10-14 11:41:26 ")
    opt[String]("queue").optional().action((queue, c) => { c.copy(queue = queue) })
      .text("Queue (optional, default=produzione)")
  }

  def main(args: Array[String]) = {
    var retCode: Integer = -1
    try {
      System.setProperty("java.security.auth.login.config", "jaas-client_sdpbatch.conf");
      System.setProperty("spark.driver.allowMultipleContexts", "true");

      LOG.info("[[MetadataPgToHive::main]] BEGIN")
      var pg_url: String = ""
      var pg_username: String = ""
      var pg_schema: String = ""
      var pg_password: String = ""
      var timestamp: String = ""
      var queue:Option[String] = None
      
      parser.parse(args, CliOptions()) match {
        case None =>
        // syntax error on command line
        case Some(opts) => {
          pg_url = opts.pg_url
          pg_username = opts.pg_username
          pg_schema = opts.pg_schema
          pg_password = opts.pg_password
          timestamp = opts.timestamp
          queue = Some(opts.queue)
        }
      }


      LOG.info("[[MetadataPgToHive::main]] pg_url --> " + pg_url)
      LOG.info("[[MetadataPgToHive::main]] pg_username --> " + pg_username)
      LOG.info("[[MetadataPgToHive::main]] pg_schema --> " + pg_schema)
      LOG.info("[[MetadataPgToHive::main]] timestamp --> " + timestamp)

      val valueQueue: String = queue match {
        case None => "produzione"
        case Some(s: String) => s
      }

      
      val conf = new SparkConf().set("spark.yarn.queue", valueQueue).setExecutorEnv(Array(("java.security.auth.login.config", "jaas-client.conf")));
      val sparkContext = new SparkContext(conf)
      val sqlContextHiveHive = new org.apache.spark.sql.hive.HiveContext(sparkContext)
      val ts = lit(timestamp).cast("timestamp")
      
      var meta_tenant_today=sqlContextHiveHive.emptyDataFrame
      var meta_tenant_toappend=sqlContextHiveHive.emptyDataFrame
      var strSql=""
        strSql=s"(select tenantcode, name tenantname, t.id_tenant idtenant, t.description descrtenant, o.organizationcode organizationcode, ty.tenanttypecode tenanttype, st.tenantstatuscode coddeploymentstatus, activationdate dataattivazione, id_ecosystem idecosystem, username, userlastname, userfirstname, useremail, usertypeauth,0 colldatasize,0 colldataindexsize,0 collmeasuressize,0 collmeasuresindexsize,0 collsocialsize,0 collsocialindexsize,0 collmediasize,0 collmediaindexsize,0 collarchivedatasize,0 collarchivedataindexsize,0 collarchivemeasuressize,0 collarchivemeasuresindexsize,cast('' as text)  databasenames,0 colltotaldbsize,0 colltotaldbindexsize, deactivationdate datadisattivazione, hasstage, zeppelin FROM $pg_schema.yucca_tenant t, yucca_organization o, yucca_d_tenant_type ty, yucca_d_tenant_status st, yucca_bundles yb , yucca_r_tenant_bundles ryb where o.id_organization=t.id_organization and ty.id_tenant_type=t.id_tenant_type and t.id_tenant_status = st.id_tenant_status and ryb.id_tenant=t.id_tenant and ryb.id_bundles = yb.id_bundles   ) a"
      meta_tenant_today= sqlContextHiveHive.read.format("jdbc").
            options(Map("driver"->"org.postgresql.Driver","url" -> pg_url,"user"->pg_username,"password"->pg_password,"dbtable" -> strSql)).load
      meta_tenant_today.write.mode("overwrite").format("orc").saveAsTable("yucca_metadata.meta_tenant")

      meta_tenant_toappend = meta_tenant_today.withColumn("ts_inserimento", ts)
      meta_tenant_toappend.write.mode("append").format("orc").saveAsTable("yucca_metadata.meta_tenant_storico")

      
      var meta_stream_today=sqlContextHiveHive.emptyDataFrame
      var meta_stream_toappend=sqlContextHiveHive.emptyDataFrame
      strSql=s"(SELECT   yucca_stream.idstream,   yucca_stream.streamcode,   yucca_stream.streamname,   yucca_r_tenant_data_source.id_tenant idtenant,yucca_tenant.tenantcode,   yucca_dataset.iddataset,   yucca_dataset.datasourceversion datasetversion,   yucca_smart_object.id_smart_object idvirtualentity,   yucca_smart_object.id_so_category idcategoriave,   yucca_smart_object.id_so_type idtipove,   yucca_smart_object.name virtualentityname,   yucca_smart_object.description descrve,   yucca_smart_object.socode virtualentitycode,   yucca_d_so_type.sotypecode virtualentitytype,   yucca_d_so_category.socategorycode virtualentitycategory,   yucca_d_status.statuscode  streamstatus,   yucca_d_domain.domaincode domainstream,   yucca_d_license.licensecode licence,   yucca_data_source.visibility,  cast('true' as text) publishstream,  (select array_to_string(array_agg(yucca_component.name),',') from yucca_component	where yucca_data_source.id_data_source = yucca_component.id_data_source AND yucca_data_source.datasourceversion = yucca_component.datasourceversion	)  elencocomponent,   (select array_to_string(array_agg(yucca_d_tag.tagcode),',') from yucca_r_tag_data_source 			, yucca_d_tag 			where yucca_data_source.id_data_source = yucca_r_tag_data_source.id_data_source AND 			yucca_data_source.datasourceversion = yucca_r_tag_data_source.datasourceversion 			and yucca_r_tag_data_source.id_tag = yucca_d_tag.id_tag) elencotag,  (select array_to_string(array_agg(yucca_tenant.tenantcode),',') from yucca_r_tenant_data_source, yucca_tenant	where yucca_data_source.id_data_source = yucca_r_tenant_data_source.id_data_source AND 	yucca_data_source.datasourceversion = yucca_r_tenant_data_source.datasourceversion and yucca_tenant.id_tenant = yucca_r_tenant_data_source.id_tenant and yucca_r_tenant_data_source.isactive=1)  elencoshare,   yucca_smart_object.slug virtualentityslug  FROM   yucca_stream,    yucca_data_source 	left join yucca_dataset on (yucca_dataset.id_data_source = yucca_data_source.id_data_source AND yucca_dataset.datasourceversion = yucca_data_source.datasourceversion) 	left join yucca_d_license on (yucca_data_source.id_license = yucca_d_license.id_license),  yucca_r_tenant_data_source,     yucca_smart_object,    yucca_d_so_type,   yucca_d_so_category,   yucca_d_status,   yucca_d_domain,   yucca_d_subdomain,yucca_tenant   WHERE   yucca_stream.id_data_source = yucca_data_source.id_data_source AND   yucca_stream.datasourceversion = yucca_data_source.datasourceversion   AND   yucca_stream.id_smart_object = yucca_smart_object.id_smart_object AND   yucca_data_source.id_data_source = yucca_r_tenant_data_source.id_data_source   AND   yucca_data_source.datasourceversion = yucca_r_tenant_data_source.datasourceversion   AND     yucca_smart_object.id_so_type = yucca_d_so_type.id_so_type AND   yucca_smart_object.id_so_category = yucca_d_so_category.id_so_category   AND   yucca_d_status.id_status = yucca_data_source.id_status AND   yucca_d_subdomain.id_domain = yucca_d_domain.id_domain   AND   yucca_d_subdomain.id_subdomain = yucca_data_source.id_subdomain AND   yucca_r_tenant_data_source.ismanager = 1   AND   yucca_r_tenant_data_source.isactive = 1 AND yucca_tenant.id_tenant = yucca_r_tenant_data_source.id_tenant ) a"
      meta_stream_today= sqlContextHiveHive.read.format("jdbc").
            options(Map("driver"->"org.postgresql.Driver","url" -> pg_url,"user"->pg_username,"password"->pg_password,"dbtable" -> strSql)).load
      meta_stream_today.write.mode("overwrite").format("orc").saveAsTable("yucca_metadata.meta_stream")
      
      meta_stream_toappend = meta_stream_today.withColumn("ts_inserimento", ts)
      meta_stream_toappend.write.mode("append").format("orc").saveAsTable("yucca_metadata.meta_stream_storico")
      
      
      var meta_dataset_today=sqlContextHiveHive.emptyDataFrame
      var meta_dataset_toappend=sqlContextHiveHive.emptyDataFrame
      strSql=s"(SELECT   yucca_dataset.iddataset,   yucca_dataset.datasetcode,cast(yucca_dataset.datasourceversion as varchar) datasetversion, yucca_r_tenant_data_source.id_tenant idtenant,yucca_tenant.tenantcode,     dataset_type tipo, dataset_subtype sottotipo,phoenixtablename  tabella,  yucca_d_status.statuscode  datasetstatus,  CASE WHEN yucca_dataset.datasourceversion = ( select max(dd.datasourceversion) from yucca_data_source dd where dd.id_data_source = yucca_data_source.id_data_source) AND yucca_d_status.statuscode = 'inst' THEN 1 ELSE 0 END corrente, datasetname, yucca_dataset.description descrds,   yucca_d_license.licensecode license,   yucca_data_source.visibility,    yucca_d_domain.domaincode datadomain, (select byd.iddataset from yucca_dataset byd where byd.id_data_source=yucca_dataset.id_data_source_binary) binaryiddataset,   datasourceversion_binary binarydatasetversion, isopendata openflag, opendataauthor openauthor, (select array_to_string(array_agg(yucca_component.name),',') from yucca_component 			where yucca_data_source.id_data_source = yucca_component.id_data_source AND 			yucca_data_source.datasourceversion = yucca_component.datasourceversion 			)  elencocampi,     (select array_to_string(array_agg(yucca_d_tag.tagcode),',') from yucca_r_tag_data_source 			, yucca_d_tag 			where yucca_data_source.id_data_source = yucca_r_tag_data_source.id_data_source AND 			yucca_data_source.datasourceversion = yucca_r_tag_data_source.datasourceversion 			and yucca_r_tag_data_source.id_tag = yucca_d_tag.id_tag) elencotag,  (select array_to_string(array_agg(yucca_tenant.tenantcode),',') from yucca_r_tenant_data_source 			, yucca_tenant 			where yucca_data_source.id_data_source = yucca_r_tenant_data_source.id_data_source AND 			yucca_data_source.datasourceversion = yucca_r_tenant_data_source.datasourceversion 			and yucca_tenant.id_tenant = yucca_r_tenant_data_source.id_tenant and yucca_r_tenant_data_source.isactive=1) elencoshare,yucca_d_subdomain.subdomaincode codsubdomain, 			CASE WHEN (yucca_d_status.statuscode = 'inst') THEN 'false' ELSE 'true' END deleted 			FROM   yucca_data_source	left join yucca_dataset 			on (yucca_dataset.id_data_source = yucca_data_source.id_data_source AND yucca_dataset.datasourceversion = yucca_data_source.datasourceversion) 	 			left join yucca_d_license on (yucca_data_source.id_license = yucca_d_license.id_license),  yucca_r_tenant_data_source,   			  yucca_d_status,   yucca_d_domain,   yucca_d_subdomain,yucca_tenant  , yucca_d_dataset_type dt, yucca_d_dataset_subtype st 			   WHERE      yucca_data_source.id_data_source = yucca_r_tenant_data_source.id_data_source   AND   yucca_data_source.datasourceversion = yucca_r_tenant_data_source.datasourceversion  			   AND   yucca_d_status.id_status = yucca_data_source.id_status AND   yucca_d_subdomain.id_domain = yucca_d_domain.id_domain   AND   yucca_d_subdomain.id_subdomain = yucca_data_source.id_subdomain 			   AND   yucca_r_tenant_data_source.ismanager = 1   AND   yucca_r_tenant_data_source.isactive = 1 AND yucca_tenant.id_tenant = yucca_r_tenant_data_source.id_tenant 			   and yucca_dataset.id_dataset_type = dt.id_dataset_type AND yucca_dataset.id_dataset_subtype = st.id_dataset_subtype  )a "
      meta_dataset_today= sqlContextHiveHive.read.format("jdbc").
            options(Map("driver"->"org.postgresql.Driver","url" -> pg_url,"user"->pg_username,"password"->pg_password,"dbtable" -> strSql)).load
      meta_dataset_today.write.mode("overwrite").format("orc").saveAsTable("yucca_metadata.meta_dataset")
      meta_dataset_toappend = meta_dataset_today.withColumn("ts_inserimento", ts)
      meta_dataset_toappend.write.mode("append").format("orc").saveAsTable("yucca_metadata.meta_dataset_storico")
      
      
      var meta_dataset_extended_today=sqlContextHiveHive.emptyDataFrame
      var meta_dataset_extended_toappend=sqlContextHiveHive.emptyDataFrame
      strSql=s"(SELECT   yucca_dataset.iddataset,   yucca_dataset.datasetcode,cast(yucca_dataset.datasourceversion as varchar) datasetversion, yucca_r_tenant_data_source.id_tenant idtenant,yucca_tenant.tenantcode,     dataset_type tipo, dataset_subtype sottotipo,phoenixtablename  tabella,  yucca_d_status.statuscode  datasetstatus,  CASE WHEN yucca_dataset.datasourceversion = ( select max(dd.datasourceversion) from yucca_data_source dd where dd.id_data_source = yucca_data_source.id_data_source) AND yucca_d_status.statuscode = 'inst' THEN 1 ELSE 0 END corrente, datasetname, yucca_dataset.description descrds,   yucca_d_license.licensecode license,   yucca_data_source.visibility,    yucca_d_domain.domaincode datadomain, (select byd.iddataset from yucca_dataset byd where byd.id_data_source=yucca_dataset.id_data_source_binary) binaryiddataset,   datasourceversion_binary binarydatasetversion, isopendata openflag, opendataauthor openauthor, (select array_to_string(array_agg(yucca_component.name),',') from yucca_component 			where yucca_data_source.id_data_source = yucca_component.id_data_source AND 			yucca_data_source.datasourceversion = yucca_component.datasourceversion 			)  elencocampi,     (select array_to_string(array_agg(yucca_d_tag.tagcode),',') from yucca_r_tag_data_source 			, yucca_d_tag 			where yucca_data_source.id_data_source = yucca_r_tag_data_source.id_data_source AND 			yucca_data_source.datasourceversion = yucca_r_tag_data_source.datasourceversion 			and yucca_r_tag_data_source.id_tag = yucca_d_tag.id_tag) elencotag,  (select array_to_string(array_agg(yucca_tenant.tenantcode),',') from yucca_r_tenant_data_source 			, yucca_tenant 			where yucca_data_source.id_data_source = yucca_r_tenant_data_source.id_data_source AND 			yucca_data_source.datasourceversion = yucca_r_tenant_data_source.datasourceversion 			and yucca_tenant.id_tenant = yucca_r_tenant_data_source.id_tenant and yucca_r_tenant_data_source.isactive=1) elencoshare,yucca_d_subdomain.subdomaincode codsubdomain, 			CASE WHEN (yucca_d_status.statuscode = 'inst') THEN 'false' ELSE 'true' END deleted, case  st.dataset_subtype 			  when 'bulkDataset' then coalesce(yucca_dataset.solrcollectionname, yucca_organization.datasolrcollectionname) 			 when 'socialDataset' then coalesce(yucca_dataset.solrcollectionname, yucca_organization.socialsolrcollectionname) 			  when 'streamDataset' then coalesce(yucca_dataset.solrcollectionname, yucca_organization.measuresolrcollectionname) 			else coalesce(yucca_dataset.solrcollectionname, yucca_organization.mediasolrcollectionname) 			     end solrcollectionname, 			 case  st.dataset_subtype 			when 'bulkDataset' then coalesce(yucca_dataset.phoenixtablename, yucca_organization.dataphoenixtablename) 		   when 'socialDataset' then coalesce(yucca_dataset.phoenixtablename, yucca_organization.socialphoenixtablename) 			when 'streamDataset' then coalesce(yucca_dataset.phoenixtablename, yucca_organization.measuresphoenixtablename) 			else coalesce(yucca_dataset.phoenixtablename, yucca_organization.mediaphoenixtablename) 			     end phoenixtablename, 			 case  st.dataset_subtype   			when 'bulkDataset' then coalesce(yucca_dataset.phoenixschemaname, yucca_organization.dataphoenixschemaname) 			when 'socialDataset' then coalesce(yucca_dataset.phoenixschemaname, yucca_organization.socialphoenixschemaname) 			when 'streamDataset' then coalesce(yucca_dataset.phoenixschemaname, yucca_organization.measuresphoenixschemaname) 			else coalesce(yucca_dataset.phoenixschemaname, yucca_organization.mediaphoenixschemaname) 			     end phoenixschemaname, yucca_dataset.dbhiveschema, yucca_dataset.dbhivetable     			FROM   yucca_data_source	left join yucca_dataset 			on (yucca_dataset.id_data_source = yucca_data_source.id_data_source AND yucca_dataset.datasourceversion = yucca_data_source.datasourceversion) 	 			left join yucca_d_license on (yucca_data_source.id_license = yucca_d_license.id_license),  yucca_r_tenant_data_source,   			  yucca_d_status,   yucca_d_domain,   yucca_d_subdomain,yucca_tenant  , yucca_d_dataset_type dt, yucca_d_dataset_subtype st , yucca_organization				   WHERE      yucca_data_source.id_data_source = yucca_r_tenant_data_source.id_data_source   AND   yucca_data_source.datasourceversion = yucca_r_tenant_data_source.datasourceversion  			   AND   yucca_d_status.id_status = yucca_data_source.id_status AND   yucca_d_subdomain.id_domain = yucca_d_domain.id_domain   AND   yucca_d_subdomain.id_subdomain = yucca_data_source.id_subdomain 			   AND   yucca_r_tenant_data_source.ismanager = 1   AND   yucca_r_tenant_data_source.isactive = 1 AND yucca_tenant.id_tenant = yucca_r_tenant_data_source.id_tenant 			   and yucca_dataset.id_dataset_type = dt.id_dataset_type AND yucca_dataset.id_dataset_subtype = st.id_dataset_subtype and yucca_data_source.id_organization = yucca_organization.id_organization  )a "
      meta_dataset_extended_today= sqlContextHiveHive.read.format("jdbc").
            options(Map("driver"->"org.postgresql.Driver","url" -> pg_url,"user"->pg_username,"password"->pg_password,"dbtable" -> strSql)).load
      meta_dataset_extended_today.write.mode("overwrite").format("orc").saveAsTable("yucca_metadata.meta_dataset_extended")
      meta_dataset_extended_toappend = meta_dataset_extended_today.withColumn("ts_inserimento", ts)
      meta_dataset_extended_toappend.write.mode("append").format("orc").saveAsTable("yucca_metadata.meta_dataset_extended_storico")
      
      
//      var meta_api_today=sqlContextHiveHive.emptyDataFrame
//      var meta_api_toappend=sqlContextHiveHive.emptyDataFrame
//      strSql=s"(SELECT   yucca_api.idapi,   yucca_api.apiname,   yucca_api.apicode,   yucca_tenant.id_tenant,   yucca_tenant.tenantcode,   yucca_api.apitype,   yucca_api.apisubtype,   yucca_api.entitynamespace,   yucca_dataset.iddataset,   yucca_data_source.datasourceversion,   yucca_stream.idstream,   yucca_stream.streamcode,   yucca_smart_object.socode FROM   yucca_api,   yucca_data_source left join   yucca_dataset on (yucca_data_source.id_data_source = yucca_dataset.id_data_source AND   yucca_data_source.datasourceversion = yucca_dataset.datasourceversion) 	left join yucca_stream on (yucca_data_source.id_data_source = yucca_stream.id_data_source AND 					 yucca_data_source.datasourceversion = yucca_stream.datasourceversion) 	left join  yucca_smart_object on (yucca_stream.id_smart_object = yucca_smart_object.id_smart_object),   yucca_r_tenant_data_source,   yucca_tenant WHERE   yucca_api.id_data_source = yucca_data_source.id_data_source AND   yucca_api.datasourceversion = yucca_data_source.datasourceversion AND   yucca_r_tenant_data_source.id_data_source = yucca_data_source.id_data_source AND   yucca_r_tenant_data_source.datasourceversion = yucca_data_source.datasourceversion AND   yucca_tenant.id_tenant = yucca_r_tenant_data_source.id_tenant )a "
//      meta_api_today= sqlContextHiveHive.read.format("jdbc").
//            options(Map("driver"->"org.postgresql.Driver","url" -> pg_url,"user"->pg_username,"password"->pg_password,"dbtable" -> strSql)).load
//      meta_api_today.write.mode("overwrite").format("orc").saveAsTable("yucca_metadata.meta_api")
//      meta_api_toappend = meta_api_today.withColumn("ts_inserimento", ts)
//      meta_api_toappend.write.mode("append").format("orc").saveAsTable("yucca_metadata.meta_api_storico")
      
    } catch {
      case e: Exception =>
        LOG.error("[[MetadataPgToHive::main]]" + e, e)
        throw e
    } finally {
      LOG.info("[[MetadataPgToHive::main]] END")

    }

    System.exit(retCode)

  }


}