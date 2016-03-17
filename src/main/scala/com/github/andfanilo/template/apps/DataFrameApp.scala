package com.github.andfanilo.template.apps

import com.github.andfanilo.template.AbstractApp
import org.apache.spark.sql.functions._

object DataFrameApp extends AbstractApp {

  override def execute(args: Array[String]) {
    val filePath = "src/main/resources/data/FL_insurance_sample.csv"
    val df = sqlContext.read.format("com.databricks.spark.csv").options(Map(
      "header" -> "true",
      "delimiter" -> ",",
      "inferSchema" -> "true"
    )).load(filePath)

    df.show()
    df.printSchema()

    /* Some selection and adding columns */
    df.select("policyID", "county").withColumn("zeros", lit(0)).show()
    df.select("policyID", "county").withColumn("policyID*2", df("policyID") * 2).show()
    df.select("policyID", "county").withColumn("is_policyID>20000", df("policyID") > 20000).show()

    /* Some filtering */
    df.select(df("policyID"), (df("policyID") > 20000 && df("policyID") < 30000).alias("policyID_test")).show()

    /* Some aggregation */
    df.groupBy(df("county")).avg("eq_site_limit")
    df.groupBy(df("county")).agg(
      avg("eq_site_limit").alias("average"),
      min("eq_site_limit").alias("minimum"),
      max("eq_site_limit").alias("maximum")
    ).show()

    /* Some windowing, you'd need a HiveContext to run it*/
    // import org.apache.spark.sql.expressions.Window
    // val window = Window.partitionBy("county").orderBy("policyID")
    // df.select("policyID", "county", "eq_site_limit").withColumn("diff", lead("eq_site_limit", 1).over(window)).show()
  }
}