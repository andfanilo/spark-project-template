package com.github.fandetwa.template

import java.io.File

import com.github.fandetwa.template.spark.SparkRegistrator
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object App {

  def main(args: Array[String]) {
    val (sc, sqlContext) = loadContexts()

    val filePath = "src/main/resources/data/FL_insurance_sample.csv"
    val df = sqlContext.read.format("com.databricks.spark.csv").options(Map(
      "header" -> "true",
      "delimiter" -> ","
    )).load(filePath)

    df.show()
    df.printSchema()

    sc.stop()
  }

  /**
   * Create a SparkContext and associated SQLContext
   * @return tuple (SparkContext, associated SQLContext)
   */
  private def loadContexts(): (SparkContext, SQLContext) = {
    loadWinUtils()
    val conf = new SparkConf()
    conf
      .setMaster("local[*]")
      .setAppName("template")
      .set("spark.serializer", classOf[KryoSerializer].getCanonicalName)
      .set("spark.kryo.registrator", classOf[SparkRegistrator].getCanonicalName)
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    (sc, sqlContext)
  }

  /**
   * Need to load winutils.exe when developing
   * https://issues.apache.org/jira/browse/SPARK-2356
   */
  private def loadWinUtils(): Unit = {
    val sep = File.separator
    val current = new File(".").getAbsolutePath.replace(s"$sep.", "")
    System.setProperty("hadoop.home.dir", s"$current${sep}src${sep}test${sep}resources$sep")
  }
}