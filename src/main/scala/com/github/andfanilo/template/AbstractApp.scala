package com.github.andfanilo.template

import com.github.andfanilo.template.spark.{SparkContextProvider, SparkRegistrator}
import com.github.andfanilo.template.utils.WinUtilsLoader

/**
  * Base class for implementing Spark code. Manages the SparkSession at beginning and end of code
  */
abstract class AbstractApp extends SparkContextProvider {

  /**
    * Main entry point for the application, it starts the SparkSession before execute() and stops it after execute()
    *
    * @param args program arguments
    */
  def main(args: Array[String]): Unit = {
    WinUtilsLoader.loadWinUtils()
    createSparkSession("local[*]", this.getClass.toString, classOf[SparkRegistrator])
    execute(args)
    stopSparkSession()
  }

  /**
    * Override this to add functionality to your application
    */
  def execute(args: Array[String])
}
