package com.github.fandetwa.template

import java.io.File

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object App {

  def main(args: Array[String]) {
    loadWinUtils()

    // Load Spark contexts
    val sc = new SparkContext("local[*]", "sandbox")
    val sqlContext = new SQLContext(sc)

    // Create DataFrame from sample csv file
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
   * Need to load winutils.exe
   * https://issues.apache.org/jira/browse/SPARK-2356
   */
  private def loadWinUtils(): Unit = {
    val sep = File.separator
    val current = new File(".").getAbsolutePath.replace(s"$sep.", "")
    System.setProperty("hadoop.home.dir", s"$current${sep}src${sep}main${sep}resources$sep")
  }
}