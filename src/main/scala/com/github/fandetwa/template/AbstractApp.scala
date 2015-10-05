package com.github.fandetwa.template

import com.github.fandetwa.template.spark.{SparkContextProvider, SparkRegistrator}

/**
 * Base class for implementing Spark code. Manages the SparkContext at beginning and end of code
 */
abstract class AbstractApp extends SparkContextProvider {

  /**
   * Main entry point for the application, it starts the SparkContext before execute() and stops it after execute()
   * @param args program arguments
   */
  def main(args: Array[String]): Unit = {
    createContext("local[*]", this.getClass.toString, classOf[SparkRegistrator])
    execute(args)
    stopContext()
  }

  /**
   * Override this to add functionality to your application
   */
  def execute(args: Array[String])
}
