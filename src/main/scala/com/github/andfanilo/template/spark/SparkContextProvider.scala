package com.github.andfanilo.template.spark

import com.github.andfanilo.template.utils.Logging
import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession

/**
  * Trait that provides the management of a SparkSession
  */
trait SparkContextProvider extends Logging {

  @transient private var _sparkSession: SparkSession = _

  /**
    * Create a single accessible SparkContext
    *
    * @param contextType         type of job
    * @param contextName         name of job
    * @param serializerClass     Kryo Registrator used, defaults to SparkRegistrator
    * @param kryoBuffer          kryo buffer, defaults to 8M
    * @param eventLogEnabled     save log files, defaults to false
    * @param applicationSparkLog where to save log files for future access, defaults to file:///tmp/spark-events
    * @param sqlJoinPartitions   how mant partitions to use for DataFrame shuffles
    * @tparam T this is for serializerClass, which is of type Class[T]
    * @return Spark session
    */
  def createSparkSession[T](contextType: String,
                            contextName: String,
                            serializerClass: Class[T],
                            kryoBuffer: String = "8M",
                            eventLogEnabled: Boolean = false,
                            applicationSparkLog: String = "file:///tmp/spark-events",
                            sqlJoinPartitions: Int = 10
                      ): SparkSession = {
    val conf = new SparkConf()
    conf
      .setMaster(contextType)
      .setAppName(contextName)
      .set("spark.serializer", classOf[KryoSerializer].getCanonicalName)
      .set("spark.kryo.registrator", serializerClass.getCanonicalName)
      .set("spark.kryoserializer.buffer", kryoBuffer)
      .set("spark.hadoop.validateOutputSpecs", "false")
      .set("spark.sql.shuffle.partitions", sqlJoinPartitions.toString)
    logInfo(s"Created new SparkContext of type $contextType named $contextName")
    _sparkSession = SparkSession
      .builder()
      .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse")
      .config(conf)
      .getOrCreate()

    sparkSession
  }

  def sparkSession: SparkSession = _sparkSession

  /**
    * Stop the Spark session and clear driver ports
    *
    */
  def stopSparkSession() {
    if (_sparkSession != null) {
      _sparkSession.stop()
    }
    // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
    System.clearProperty("spark.driver.port")
    logInfo(s"Stopped SparkSession")
  }
}
