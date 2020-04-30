package org.csi.yucca.yuccalogalignment

object QuerySql {
  
  val insertUnionJbossOdata = 
    "insert into table db_csi_log.OUTPUT_TABLE partition(year, month, day) "+ 
"select  "+
"'odataintapi1' as servercode,t2.log_timestamp,t2.uniqueid,t1.forwardedfor,t1.jwt,t2.path,t2.apicode,t2.datasetcode,t2.tenant, "+ 
"t1.query,t2.nrecin,t2.nrecout,t1.elapsed,t1.error e1_bsn,t1.error_servlet e1_servlet,t2.error e2_bsn,t2.error_servlet e2_servlet, "+
"t2.unix_ts ,year(t2.log_timestamp) ,month(t2.log_timestamp),day(t2.log_timestamp) "+
"from db_csi_log.INPUT_TABLE t1 "+
"  INNER JOIN ( "+
"	select o1.log_timestamp,o1.uniqueid,o1.path, o1.apicode,o1.forwardedfor,o1.nrecin, o1.nrecout, o1.error, o1.error_servlet,o1.unix_ts, "+
"		case when o1.tenant = '-' then d1.tenantcode  "+
"			 when o1.tenant = '|' then d1.tenantcode  "+
"			 else o1.tenant "+
"	    end as tenant, "+
"		case when o1.datasetcode = '-' then d1.datasetcode "+ 
"			 when o1.datasetcode = '|' then d1.datasetcode  "+
"			 else o1.datasetcode "+
"	    end as datasetcode "+
"		from db_csi_log.INPUT_TABLE o1, yucca_metadata.meta_dataset d1 where "+ 
"			o1.apicode = d1.datasetcode	 "+
"		) t2  "+
"  on t1.uniqueid = t2.uniqueid "+
"where t1.forwardedfor <> '-'  AND t2.forwardedfor = '-'  ";

  val fromIntapi1ToJboss = insertUnionJbossOdata.replace("INPUT_TABLE", "sdnet_intapi1_odata").replace("OUTPUT_TABLE","sdnet_odata_jboss_partday");
  val fromIntapi2ToJboss = insertUnionJbossOdata.replace("INPUT_TABLE", "sdnet_intapi2_odata").replace("OUTPUT_TABLE","sdnet_odata_jboss_partday");
  
}