package org.csi.yucca.yuccalogalignment
import org.slf4j.LoggerFactory
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive._
import org.apache.spark.sql.functions._
import scopt._
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FileUtil
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.permission.FsPermission
import java.nio.file.Files
import org.apache.commons.io.FileUtils
import java.io.File
import org.apache.commons.io.filefilter.FileFilterUtils
import java.io.FileOutputStream
import java.io.BufferedOutputStream
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream
import org.apache.commons.compress.archivers.tar.TarArchiveEntry
import org.apache.commons.compress.utils.IOUtils
import java.io.FileInputStream
import scala.collection.immutable
import org.apache.spark.sql.SaveMode

case class TableFromCsv(name: String,filename: String,  fields: Seq[String])

object YuccaLogToHive {

  val LOG = LoggerFactory.getLogger(getClass)

  val fieldsTablesMaps = immutable.Map (
      "odataTable" -> Seq(
          "id_api","forwarded_for","jwt","local_ip","host_port","client_ip",
          "remote_logname","remote_user","log_timestamp","risorsa","query",
          "status_ret","refer","useragent","byte_in","byte_out","elapsed_microsec"
          ),
      "inputAmqTable" -> Seq(
          "log_timestamp","uniqueid","connectionId","protocol","username","tenant",
          "destination","operation","ipOrigin","sensorStream","numeroEventi","elapsed",
          "error"
          ),
      "inputEsbTable" -> Seq(
          "log_timestamp","uniqueid","connid","protocol","tenant","destination",
          "operation","iporigin","sensor_stream","numevents","elapsed","error"
         ),
      "intapiOdataTable" -> Seq(
          "log_timestamp","uniqueid","forwardedfor","jwt","path","apicode","datasetcode",
          "tenant","query","nrecin","nrecout","elapsed","error_bsn","error_servlet"
         )
  )
  
  val csvTables = immutable.Map (
        "sdnet_intapi" -> TableFromCsv("sdnet_intapi","sdnet-intapi",fieldsTablesMaps.get("odataTable").get)
       ,"sdnet_stream1_apache" -> TableFromCsv("sdnet_stream1_apache","sdnet-stream1-apache",fieldsTablesMaps.get("odataTable").get),
        "sdnet_stream2_apache" -> TableFromCsv("sdnet_stream2_apache","sdnet-stream2-apache",fieldsTablesMaps.get("odataTable").get),
        "sdnet_web1" -> TableFromCsv("sdnet_web1","sdnet-web1",fieldsTablesMaps.get("odataTable").get),
        "sdnet_web2" -> TableFromCsv("sdnet_web2","sdnet-web2",fieldsTablesMaps.get("odataTable").get),
        
        
        "sdnet_stream1_amq"-> TableFromCsv("sdnet_stream1_amq","sdnet-stream1-amq",fieldsTablesMaps.get("inputAmqTable").get),
        "sdnet_stream2_amq"-> TableFromCsv("sdnet_stream2_amq","sdnet-stream2-amq",fieldsTablesMaps.get("inputAmqTable").get),
        
        "sdnet_esbin1"-> TableFromCsv("sdnet_esbin1","sdnet-esbin1",fieldsTablesMaps.get("inputEsbTable").get),
        "sdnet_esbin2"-> TableFromCsv("sdnet_esbin2","sdnet-esbin2",fieldsTablesMaps.get("inputEsbTable").get),
        
        "sdnet_intapi1_ajb620"-> TableFromCsv("sdnet_intapi1_ajb620","sdnet-intapi1-ajb620",fieldsTablesMaps.get("intapiOdataTable").get),
        "sdnet_intapi2_ajb620"-> TableFromCsv("sdnet_intapi2_ajb620","sdnet-intapi2-ajb620",fieldsTablesMaps.get("intapiOdataTable").get),
        "sdnet_intapi1_odata"-> TableFromCsv("sdnet_intapi1_odata","sdnet-intapi1-odata",fieldsTablesMaps.get("intapiOdataTable").get),
        "sdnet_intapi2_odata"-> TableFromCsv("sdnet_intapi2_odata","sdnet-intapi2-odata",fieldsTablesMaps.get("intapiOdataTable").get)
        
  )
  
  
  
  case class CliOptions(
      timestamp: String = "",
      queue: String = "") {

  }

  val parser = new scopt.OptionParser[CliOptions]("ingest") {
    head("Metadata To Hive Cli ", "0.x")

    opt[String]("timestamp").required().action((timestamp, c) => { c.copy(timestamp = timestamp) })
      .text("Timestamp in form 2018-10-14 11:41:26 ")
 opt[String]("queue").optional().action((queue, c) => { c.copy(queue = queue) })
      .text("Queue (optional, default=produzione)")
  }

  def main(args: Array[String]) = {
    var retCode: Integer = -1
    try {
      System.setProperty("spark.driver.allowMultipleContexts", "true");

      LOG.info("[[YuccaLogToHive::main]] BEGIN")
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

      LOG.info("[[YuccaLogToHive::main]] timestamp --> " + timestamp)
      val valueQueue: String = queue match {
        case None => "produzione"
        case Some(s: String) => s
      }
      val conf = new SparkConf().set("spark.yarn.queue", valueQueue)
          .setExecutorEnv(Array(("java.security.auth.login.config", "jaas-client.conf")))
          .set("spark.submit.deployMode", "client");
      val sparkContext = new SparkContext(conf)
      val sqlContextHiveHive = new org.apache.spark.sql.hive.HiveContext(sparkContext)
      sqlContextHiveHive.setConf("hive.exec.dynamic.partition", "true") 
      sqlContextHiveHive.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
      val ts = lit(timestamp).cast("timestamp")

      LOG.info("[[YuccaLogToHive::main]] carica_log_su_hdfs BEGIN")
     
      // carica_log_su_hdfs
      val hadoopFs = org.apache.hadoop.fs.FileSystem.get(sparkContext.hadoopConfiguration)
      val logRawDir = "/CSI/yucca/rawdata/log/db_log_raw"
      if (!(hadoopFs.exists(new Path(logRawDir)))) {
        hadoopFs.mkdirs(new Path(logRawDir), new FsPermission("755"))
      }
      
      val common_dir= "/wrk_common/log_yucca"
      val hdfs_root = logRawDir
      val temp_dir= "/home/sdp_batch/log_yucca/temp"
      val arch_dir= "/home/sdp_batch/log_yucca/archive"

      for ((name, table) <-csvTables)
      {
        val server = table.filename
        LOG.info("[[YuccaLogToHive::main]] Sposta file del server "+server)
        FileUtils.forceMkdir(new File(temp_dir))
        FileUtils.cleanDirectory(new File(temp_dir))
        FileUtils.copyDirectory(new File(common_dir+"/"+server), new File(temp_dir))
        FileUtils.cleanDirectory(new File(common_dir+"/"+server));

        LOG.info("[[YuccaLogToHive::main]] Archivia file in "+arch_dir+"/"+server)
        val tarGzPath = arch_dir+"/"+server+"_"+timestamp.replace(" ", "-").replace(":", "-")+".tar.gz"
        val fOut = new FileOutputStream(new File(tarGzPath))
        val bOut = new BufferedOutputStream(fOut)
        val gzOut = new GzipCompressorOutputStream(bOut)
        val tOut = new TarArchiveOutputStream(gzOut)
        try {
          val tempDirFile = new File(temp_dir)
          val children = tempDirFile.listFiles()
          if (children != null) {
              for (child <- children) {
                  val entryName = child.getName();
                  val tarEntry = new TarArchiveEntry(child, entryName);
                  tOut.putArchiveEntry(tarEntry);
                  IOUtils.copy(new FileInputStream(child), tOut);
                  tOut.closeArchiveEntry();
              }
          }
        } finally {
            tOut.finish();
            tOut.close();
            gzOut.close();
            bOut.close();
            fOut.close();
        }

        LOG.info("[[YuccaLogToHive::main]] Sposta file su hdfs in "+hdfs_root+"/"+server)

        if (!(hadoopFs.exists(new Path(hdfs_root+"/"+server)))) {          
          hadoopFs.mkdirs(new Path(hdfs_root+"/"+server), FsPermission.createImmutable(751.toShort))
        }
        hadoopFs.delete(new Path(hdfs_root+"/"+server), true)
        val children = new File(temp_dir).listFiles()
        if (children != null) {
            for (child <- children) {
              hadoopFs.copyFromLocalFile(new Path(child.getPath), 
                  new Path(hdfs_root+"/"+server,child.getName))
//        hdfs dfs -chmod 644 ${hdfs_root}/${my_server}/*
            }
        }
        FileUtils.cleanDirectory(new File(temp_dir))
        LOG.info("[[YuccaLogToHive::main]] Deleted temp files for "+server)
      } // for servers
      // carica_log_su_hdfs
      LOG.info("[[YuccaLogToHive::main]] carica_log_su_hdfs END")
      LOG.info("[[YuccaLogToHive::main]] carica_log_su_hive BEGIN")
      for ( (name, table)<-csvTables)
      {
        var readTableCsv = sqlContextHiveHive.read.format("com.databricks.spark.csv").
            option("header", "false").
            option("inferSchema", "false").
            option("delimiter",",").
            load(hdfs_root+"/"+table.filename+"/").toDF(table.fields: _*)
            readTableCsv = readTableCsv.withColumn("unix_ts", unix_timestamp(regexp_replace(substring(col("log_timestamp"),1,19),"T"," "),"yyyy-MM-dd HH:mm:ss") )
            readTableCsv = readTableCsv.withColumn("year", year(col("log_timestamp")) )
            readTableCsv = readTableCsv.withColumn("month", month(col("log_timestamp")) )
            readTableCsv = readTableCsv.withColumn("day", dayofmonth(col("log_timestamp")) )
            try {
              readTableCsv.repartition(col("year"),col("month"),col("day")).write.format("orc").partitionBy("year", "month", "day").mode(SaveMode.Append).saveAsTable("db_csi_log."+table.name+"_partday")
              hadoopFs.delete(new Path(hdfs_root+"/"+table.filename), true)
            } catch {
              case e: Exception =>
                LOG.error("[[YuccaLogToHive::main]] Error append new data to db_csi_log."+table.name+"_partday: "+e.getMessage, e)
            }
      }
      LOG.info("[[YuccaLogToHive::main]] carica_log_su_hive END")

      LOG.info("[[YuccaLogToHive::main]] SDP_accounting_odata BEGIN")
//      sqlContextHiveHive.sql(QuerySql.fromIntapi1ToJboss);
      LOG.info("[[YuccaLogToHive::main]] SDP_accounting_odata fromIntapi1ToJboss DONE!")
//      sqlContextHiveHive.sql(QuerySql.fromIntapi2ToJboss);
      LOG.info("[[YuccaLogToHive::main]] SDP_accounting_odata fromIntapi2ToJboss DONE!")
      
      
      LOG.info("[[YuccaLogToHive::main]] SDP_accounting_odata END")
      
    } catch {
      case e: Exception =>
        LOG.error("[[YuccaLogToHive::main]]" + e, e)
        throw e
    } finally {
      LOG.info("[[YuccaLogToHive::main]] END")

    }

    System.exit(retCode)

  }


}