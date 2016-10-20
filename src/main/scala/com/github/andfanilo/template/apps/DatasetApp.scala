package com.github.andfanilo.template.apps

import com.github.andfanilo.template.AbstractApp

case class Person(name: String, age: Long)

object DatasetApp extends AbstractApp {

  override def execute(args: Array[String]) {
    // TODO : find a way to pur that import in SparkContextProvider ?
    val spark = sparkSession
    import spark.implicits._

    // Encoders are created for case classes
    val caseClassDS = Seq(
      Person("Andy", 32),
      Person("Mary", 42),
      Person("Betty", 16)
    ).toDS()
    caseClassDS.show()

    val ds = caseClassDS.map(p => (p.age * 2, p.name)) // woot it infers ds as Dataset[(Long, String)] !
    ds.select("_1").show()
  }
}