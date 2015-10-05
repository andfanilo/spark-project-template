package com.github.fandetwa.template.apps

import com.github.fandetwa.template.AbstractApp

object App extends AbstractApp {

  override def execute(args: Array[String]) {
    val filePath = "src/main/resources/data/FL_insurance_sample.csv"
    val df = sqlContext.read.format("com.databricks.spark.csv").options(Map(
      "header" -> "true",
      "delimiter" -> ","
    )).load(filePath)

    df.show()
    df.printSchema()
  }
}