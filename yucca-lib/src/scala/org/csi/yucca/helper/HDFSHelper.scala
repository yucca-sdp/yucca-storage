package org.csi.yucca.helper

import org.apache.hadoop.conf.Configuration



import sys.process._
import org.slf4j.LoggerFactory
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.RemoteIterator
import org.apache.hadoop.fs.FileStatus


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
 	
 	def deleteDataSetForCsv ( hadoopConf :Configuration, organizationCode: String, 
        domain:String,subdomain:String, datasetSubType:String, datasetCode:String, streamCode:String, streamSmartObjectSlug:String) : Boolean = {
    var hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    var retCode:Boolean=false
    try {
      
      var finalDir = getFinalDir(datasetSubType, organizationCode, domain,subdomain, streamSmartObjectSlug)
      var lastDir = getLastDir(datasetSubType, datasetCode, streamCode)
      deleteCsv(hdfs, finalDir,lastDir, "*.csv");
      
			LOG.info("[[TestDeleteHDfs::main]]  .... "+finalDir + "/" + lastDir+"/*.csv   --> "  )
      
      
    } catch{ case e: Exception =>

      return false

    }
    return true
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
      //    fs.setOwner(new Path(finalDir), "sdp", "hdfs")
    }

    if (!(fs.exists(new Path(finalDir + "/" + lastDir)))) {
      fs.mkdirs(new Path(finalDir + "/" + lastDir), new FsPermission("750"))
      //    fs.setOwner(new Path(finalDir+"/"+lastDir), "sdp", "hdfs")
    }

    LOG.info("ThreadDatasetDownloadCSV.call() ==> END")
  }
  
   def deleteCsv(fs: FileSystem, finalDir: String, lastDir: String, finalfileName: String) = {

    LOG.info("DeleteCSV.call() ==> BEGIN")

    LOG.info("finalDir: " + finalDir)
    LOG.info("lastDir: " + lastDir)
    LOG.info("pathForDelete: " + finalDir + "/" + lastDir + "/" + finalfileName)
    
    
    val deletePaths = fs.globStatus(new Path(finalDir + "/" + lastDir)).map(_.getPath)
    LOG.info("pathForDeleteExist: " + deletePaths)
    deletePaths.foreach { path => fs.delete(path,true) }

    LOG.info("DeleteCSV.call() ==> END")
  }

  
}