package it.csi.compactor

import java.util.concurrent.ExecutorCompletionService
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.immutable
import scala.collection.mutable.Buffer

import org.apache.commons.lang.time.DateUtils
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.bson.types.ObjectId
import org.csi.yucca.adminapi.client.BackofficeListaClient
import org.csi.yucca.adminapi.response.OrganizationResponse
import org.csi.yucca.helper.SolrDelegate
import org.slf4j.LoggerFactory
import java.util.Date

import org.apache.spark.scheduler.{SparkListenerTaskEnd, SparkListener}

object csvCompactor {

  class TaskEndListener extends SparkListener {
    override def onTaskEnd(taskEnd: SparkListenerTaskEnd) = {
      LOG.info("taskEnd for executor w/ID #" + taskEnd.taskInfo.executorId)
    }
  }

  val ADMIN_API_URL = "<place-holder-to-admin-http-base-uri>"

  val LOG = LoggerFactory.getLogger(getClass)

  def setSystemProperties() = {
    System.setProperty("java.security.auth.login.config", "jaas-client_sdpbatch.conf");
    System.setProperty("spark.driver.allowMultipleContexts", "true");
    System.setProperty("solr.jaas.conf.path", "jaas-client_sdpbatch.conf");
  }

  def createObjectId() = {
    new ObjectId(DateUtils.addMinutes(new java.util.Date, -7)).toString()
  }

  def getSparkContext(datasetCode: String )  = {
    val conf = new SparkConf();
    if (datasetCode != null) 
    {
      conf.set("spark.executor.instances", "2")
      .set("spark.executor.memory", "6g")
      .set("spark.yarn.queue", "produzione").setExecutorEnv(Array(("java.security.auth.login.config", "jaas-client_sdpbatch.conf")));
    }
    else {
      conf.set("spark.executor.instances", "6")
      .set("spark.executor.memory", "6g")
      .set("spark.yarn.queue", "produzione").setExecutorEnv(Array(("java.security.auth.login.config", "jaas-client_sdpbatch.conf")));
    }
    val sc = new SparkContext(conf)
    sc.addSparkListener(new TaskEndListener)
    sc
  }

  def getSqlContextHive(sparkContext: SparkContext) = {
    new org.apache.spark.sql.hive.HiveContext(sparkContext)
  }

  def getOrganization(adminApiUrl: String) = {
    val organizations = BackofficeListaClient.getOrganizations(adminApiUrl, csvCompactor.toString())
    LOG.info("Trovate " + organizations.size() + " organizations.")
    organizations
  }

  def checkError(
    executorCompletionService: ExecutorCompletionService[Outcomes],
    organizations: Buffer[OrganizationResponse], config: Config) = {

    LOG.debug("CompactCSV.checkError() ==> BEGIN")

    var exit = 0

    for (organization <- organizations if config.organizations.isEmpty || (config.organizations contains organization.getOrganizationcode)) {
      val outcomes = executorCompletionService.take().get();

      if (outcomes.hasErrors) {
        LOG.error("[[CompactCSV::main]] ERROR on Tenant " + outcomes.tenantCode + " (" + outcomes.errorMessage + ")")
        exit = -1
      } else {
        LOG.info("[[CompactCSV::main]] TENANT FINISHED = " + outcomes.tenantCode)
      }
    }

    LOG.debug("CompactCSV.checkError() ==> END")
    exit

  }

  def executorShutdownAndSystemExit(executor: ExecutorService) = {
    executor.shutdown();

    if (!executor.awaitTermination(4, TimeUnit.HOURS)) {
      executor.shutdownNow();
      LOG.info("[[CompactCSV::main]]Error")
      System.exit(-1);
    }
  }

  def getSQLContext(sparkContext: SparkContext) = {
    val sqlContext = new SQLContext(sparkContext)
    sqlContext.udf.register("toTOString", (f: Float) => f.toString)
    sqlContext
  }

  case class Config(
    organizations: Seq[String] = Seq(),
    datasetCode: String = null,
    adminApiUrl: String = ADMIN_API_URL)

  val _appName = getClass.getName // should we include package name ?!

  val parser = new scopt.OptionParser[Config](_appName) {

    head(_appName, "1.0")

    opt[Seq[String]]("organizations").valueName("[organization1 [ organization2 [...]]]").optional().unbounded().action((x, config) =>
      config.copy(organizations = x)).text("opt list of orgCodes")

    opt[String]("admin-api-uri").valueName("<adminApiURI>").action((x, config) =>
      config.copy(adminApiUrl = x)).text("adminApi connection string")

    opt[String]("datasetCode").valueName("<datasetCode> (required one org)").optional().action((x, config) =>
      config.copy(datasetCode = x)).text("Optional datasetCode to download (requires one org)")

      
    checkConfig(c =>
      if ((c.datasetCode != null) &&
        c.organizations.length != 1) failure("When using a datasetCode you must choose ONE organization")
      else success)
      
  }

  def getConfig(args: Array[String]): Config = {
    parser.parse(args, Config()) match {
      case Some(config) => config
      case None => {
        throw new Exception("[[CompactCSV::main]] Error conf")
      }
    }
  }

  def main(args: Array[String]) = {

    LOG.info("#############################################################")
    LOG.info("[[CompactCSV::main]]BEGIN")
    LOG.info("#############################################################")

    val config: Config = getConfig(args)
    
    LOG.info("[[CompactCSV::main]] organization --> " + config.organizations)
    LOG.info("[[CompactCSV::main]] dataset --> " + config.datasetCode)

    setSystemProperties()

    val organizations = getOrganization(config.adminApiUrl).asScala
    val sparkContext = getSparkContext(config.datasetCode)
    val sqlContextHive = getSqlContextHive(sparkContext)
    sparkContext.hadoopConfiguration.set("mapreduce.output.fileoutputformat.compress", "false")

    val executor = Executors.newFixedThreadPool(4);

    val executorCompletionService = new ExecutorCompletionService[Outcomes](executor);

    LOG.debug("Thread Executor created!")

    val sqlContext = getSQLContext(sparkContext)
    
    val objectId =  createObjectId()

    for (
      organization <- organizations if config.organizations.isEmpty || config.organizations.contains(organization.getOrganizationcode)
    ) {
      executorCompletionService.submit(
        new MT_dataSetsCompactor(organization, config.datasetCode, sqlContext, sparkContext,config.adminApiUrl,objectId))
    }
    

    var exit = checkError(executorCompletionService, organizations, config)

    executorShutdownAndSystemExit(executor)
    LOG.info("[[CompactCSV::main]]END")
    System.exit(exit);

    LOG.info("[[CompactCSV::main]]EXIT")

  }

}

