package com.github.andfanilo.template.spark

import com.github.andfanilo.template.utils.Logging
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Trait that provides the management of a SparkContext/SQLContext
 */
trait SparkContextProvider extends Logging {

  @transient private var _sc: SparkContext = _
  @transient private var _sqlContext: SQLContext = _

  /**
   * Create a single accessible SparkContext
   * @param contextType type of job
   * @param contextName name of job
   * @param serializerClass Kryo Registrator used, defaults to SparkRegistrator
   * @param kryoBuffer kryo buffer, defaults to 8M
   * @param eventLogEnabled save log files, defaults to false
   * @param applicationSparkLog where to save log files for future access, defaults to file:///tmp/spark-events
   * @param sqlJoinPartitions how mant partitions to use for DataFrame shuffles
   * @tparam T this is for serializerClass, which is of type Class[T]
   * @return tuple of (sparkcontext, sqlcontext)
   */
  def createContext[T](contextType: String,
                       contextName: String,
                       serializerClass: Class[T] = classOf[SparkRegistrator],
                       kryoBuffer: String = "8M",
                       eventLogEnabled: Boolean = false,
                       applicationSparkLog: String = "file:///tmp/spark-events",
                       sqlJoinPartitions: Int = 10
                        ): (SparkContext, SQLContext) = {
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
    _sc = new SparkContext(conf)
    _sqlContext = new SQLContext(sc)
    (sc, sqlContext)
  }

  def sc: SparkContext = _sc

  def sqlContext: SQLContext = _sqlContext

  /**
   * Stop the SparkContext and associated SQLContext
   *
   */
  def stopContext() {
    if (_sc != null) {
      _sc.stop()
    }
    // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
    System.clearProperty("spark.driver.port")
    logInfo(s"Stopped SparkContext")
  }
}
