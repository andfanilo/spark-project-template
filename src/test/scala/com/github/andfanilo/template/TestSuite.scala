package com.github.andfanilo.template

import com.github.andfanilo.template.spark.SparkSuite
import com.github.andfanilo.template.unit.UnitSuite
import com.github.andfanilo.template.utils.WinUtilsLoader
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, Suites}

/**
  * Entry point for tests
  */
@RunWith(classOf[JUnitRunner])
class TestSuite extends Suites(
  new SparkSuite,
  new UnitSuite
) with BeforeAndAfterAll {

  override def beforeAll() {
    WinUtilsLoader.loadWinUtils()
  }

}
