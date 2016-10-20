package com.github.andfanilo.template.apps

import com.github.andfanilo.template.AbstractApp

case class Person(name: String, age: Long)

object DatasetApp extends AbstractApp {

  override def execute(args: Array[String]) {
    // TODO : find a way to pur that import in SparkContextProvider ?
    val spark = sparkSession
    import spark.implicits._

    // Encoders are created for case classes
    val caseClassDS = Seq(Person("Andy", 32)).toDS()
    caseClassDS.show()
  }
}