package org.csi.yucca.storageutils.helper
import org.apache.hadoop.conf.Configuration



import sys.process._
import org.slf4j.LoggerFactory
class HDFSHelper (val envPrefix: String){
    
  
 	val LOG = LoggerFactory.getLogger(getClass) 

  
  def deleteDataSet4promotion ( hadoopConf :Configuration, organizationCode: String, 
        domain:String,subdomain:String, datasetSubType:String, datasetCode:String, streamCode:String, streamSmartObjectSlug:String) : Boolean = {
    var hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    var retCode:Boolean=false
    try {
      
      //TODO controlli sui parametri in ingresso
      var hdfsPath="/"
      if (null!= envPrefix && envPrefix.trim().toUpperCase().equals("INTEG")) hdfsPath+="int-"
      hdfsPath+="datalake/"+organizationCode+ "/rawdata/" + domain
      
      if (datasetSubType.equals("streamDataset") || datasetSubType.equals("socialDataset")) {
        hdfsPath += "/so_" + streamSmartObjectSlug + "/" + streamCode
      } else {
        hdfsPath += "/db_" + subdomain + "/" + datasetCode
      }
			LOG.info("[[HDFSHelper::deleteDataSet4promotion]]  hdfsPath = "+ hdfsPath )
      
      //hdfsPath = "/int-datalake/PROVAPWD09/rawdata/ENVIRONMENT/so_smart02/s_02/586615e114ee90b0879f6b44_10-ds_S_02_558-1.csv"
      //retCode=hdfs.delete(new org.apache.hadoop.fs.Path(hdfsPath+"/*.csv"), true)
      //retCode=hdfs.delete(new org.apache.hadoop.fs.Path(hdfsPath), true)
      var commandStr="hdfs dfs -rm -r "+hdfsPath+"/*.csv"
      var retsh = commandStr!!
    
			LOG.info("[[HDFSHelper::deleteDataSet4promotion]]  .... retsh = "+ retsh )

			
			/*
      var cntDel:Integer=0
      for (rawWord <- retsh.split("[ ]+")) {
    if (rawWord.equals("Moved:")) {
           cntDel=cntDel+1;
         }         
     
      }          
      */
      
			LOG.info("[[TestDeleteHDfs::main]]  .... "+hdfsPath+"/*.csv   --> "  )
      
      
    } catch{ case e: Exception =>

      return false

    }
    return true
  }
  
}