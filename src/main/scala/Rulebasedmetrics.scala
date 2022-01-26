import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object Rulebasedmetrics {

  lazy val logger = LoggerFactory.getLogger(this.getClass.getName)
  lazy val dynamicAllocation: String = "spark.dynamicAllocation.enabled"
  lazy val shuffleService: String = "spark.shuffle.service.enabled"
  lazy val dynamicAllocInitialExec: String = "spark.dynamicAllocation.initialExecutors"
  lazy val minExecutors: String = "spark.dynamicAllocation.minExecutors"
  lazy val sparkSerializer: String = "spark.serializer"
  lazy val sparkJars: String = "spark.jars"
  lazy val executorMemory: String = "spark.executor.memory"
  lazy val yarnDriver: String = "spark.yarn.driver.memoryOverhead"
  lazy val yarnExecutor: String = "spark.yarn.executor.memoryOverhead"
  lazy val defaultparallism: String = "spark.default.parallelism"
  lazy val executorInstance: String = "spark.executor.instances"
  lazy val executorCores: String = "spark.executor.cores"


  def main(args: Array[String]): Unit = {
    configMetrics()

  }

  def getSparkContext(): SparkContext = {
    val master = "local[*]"
    val conf = new SparkConf().setAppName("Configuration Metrics")
      .setMaster(master)
      .set(dynamicAllocation, "false")
      .set(shuffleService, "false")
      .set(dynamicAllocInitialExec, " ")
      .set(minExecutors, "0")
      .set(sparkSerializer, "org.apache.spark.serializer.KryoSerializer")
      .set(sparkJars, "*test-jars")
      .set(executorMemory, "64g")
      .set(yarnDriver, "2g")
      .set(yarnExecutor, "2g")
      .set(defaultparallism, "100")
      .set(executorInstance, "6")
      .set(executorCores, "8")

    val sparkSession = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    return sparkSession.sparkContext
  }

  def configMetrics() = {
    var sparkContextTemp: SparkContext = getSparkContext

    /*
    * Section 1
    * Two cases in if and else below:
    * Case1 - spark.dynamicAllocation.enabled set to false
    * Case2 - spark.dynamicAllocation.enabled set to true AND spark.shuffle.service.enabled set to false
    * */

    if (sparkContextTemp.getConf.get(dynamicAllocation).toBoolean == false) {
      logger.warn(s"***Dynamic resource allocation is disabled***")
    } else if((sparkContextTemp.getConf.get(dynamicAllocation).toBoolean) == true &&
      (sparkContextTemp.getConf.get(shuffleService).toBoolean == false)) {
      logger.warn(s"***Dynamic resource allocation is enabled, but the external shuffle service is disabled ***")
    }

    //End of section----------------------------------------------------------------------------------------------

    /*
    *Section 2
    * Three cases below:
    * Case 1 - dynamic allocation misconfiguration
    * Case2 - Kyro check
    * Case3 - Wildcard jars
    * */

    if (getSparkContext().getConf.get(dynamicAllocInitialExec) == " " &&
      getSparkContext().getConf.get(minExecutors).toInt == 0) {
      logger.warn(s"*** If `--num-executors` (or `spark.executor.instances`) is set and larger than this value," +
        s" it will be used as the initial number of executors.***")
    }

    if ((getSparkContext().getConf.get(sparkSerializer) == " ") ||
      (getSparkContext().getConf.get(sparkSerializer) != "org.apache.spark.serializer.KryoSerializer")) {
      logger.warn((s"****Serializer is either not explicitly configured or not equal to " +
        s"org.apache.spark.serializer.KryoSerializer.***"))
    }

    if (getSparkContext().getConf.get(sparkJars).contains("*")) {
      logger.warn(s"***review defined jars and to replace * notation***")
    }
    //End of section----------------------------------------------------------------------------------------------

    /*
    *Section 3
    * Two cases below:
    * Case 1 - Memory overhead too low
    * Case2 - Default parallelism too low
    * */

    val tempDriverOverhead: String = getSparkContext().getConf.get(executorMemory).dropRight(1)
    val tempYarnDriver: String = getSparkContext().getConf.get(yarnDriver).dropRight(1)
    val tempYarnExecutor: String = getSparkContext().getConf.get(yarnExecutor).dropRight(1)

    if ((tempYarnDriver.toDouble < 0.1 * tempDriverOverhead.toDouble)
      || (tempYarnExecutor.toDouble < 0.1 * tempDriverOverhead.toDouble)) {
      logger.warn(s"*****To reduce the overhead memory defined*********")
    }

    val temExecCore: Double = getSparkContext().getConf.get(executorCores).toDouble
    val temExecinst: Double = getSparkContext().getConf.get(executorCores).toDouble

    if ((getSparkContext().getConf.get(executorInstance).toDouble) < (2 * temExecinst * temExecCore)) {
      logger.warn(s"***Suggest setting spark.default.parallelism to between 2*spark.executor.instances*spark.executor.cores" +
        s" and 4*spark.executor.instances*spark.executor.cores")
    }
    //End of section----------------------------------------------------------------------------------------------

    /*
    *Section 4
    * Two cases below:
    * Case 1 - Executor Memory Upper Bound too high
    * Case2 - Executor Core Upper Bound too high
    * */

    if (tempDriverOverhead.toInt >= 64) {
      logger.warn("***Each executor runs on its own JVM. Upwards of 64GB of memory and garbage collection issues can cause slowness")
    }
    if (temExecCore > 5) {
      logger.warn("***The upper bound for cores per executor. More than 5 cores per executor can degrade HDFS I/O throughput." +
        " This value can safely be increased if writing to a web-based “file system” such as S3, but significant " +
        "increases to this limit are not recommended.")
    }
    //End of section----------------------------------------------------------------------------------------------
  }
}
