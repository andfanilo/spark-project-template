package com.github.andfanilo.template.apps

import com.github.andfanilo.template.AbstractApp
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}

object MLApp extends AbstractApp {

  override def execute(args: Array[String]) {
    val filePath = "src/main/resources/data/FL_insurance_sample.csv"
    val df = sparkSession.read.format("com.databricks.spark.csv").options(Map(
      "header" -> "true",
      "delimiter" -> ",",
      "inferSchema" -> "true"
    )).load(filePath)

    val input = df.select("county", "eq_site_limit", "hu_site_limit", "fl_site_limit", "fr_site_limit")

    // String label to numeric
    val indexer = new StringIndexer()
      .setInputCol("county")
      .setOutputCol("label")

    // Assemble feature columns into one
    val assembler = new VectorAssembler()
      .setInputCols(Array("eq_site_limit", "hu_site_limit", "fl_site_limit", "fr_site_limit"))
      .setOutputCol("features")

    // Use Decision tree
    val tree = new DecisionTreeClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")

    // Pipeline the whole thing
    val pipeline = new Pipeline()
      .setStages(Array(indexer, assembler, tree))

    // Predict
    val model = pipeline.fit(input)
    val predictions = model.transform(input).select("features", "label", "prediction")

    // Select (prediction, true label) and compute test error.
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)
    println("Test Error = " + (1.0 - accuracy))

    val treeModel = model.stages(2).asInstanceOf[DecisionTreeClassificationModel]
    println("Learned classification tree model:\n" + treeModel.toDebugString)

  }
}