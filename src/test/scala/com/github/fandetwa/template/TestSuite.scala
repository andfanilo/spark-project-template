package com.github.fandetwa.template

import java.io.File

import com.github.fandetwa.template.integration.SampleIntegrationTest
import com.github.fandetwa.template.spark.RDDImplicitsTest
import org.scalatest.{BeforeAndAfterAll, Suites}

/**
 * Entry point for tests
 */
class TestSuite extends Suites(
  new RDDImplicitsTest,
  new SampleUnitTest,
  new SampleIntegrationTest
) with BeforeAndAfterAll {

  override def beforeAll() {
    // Hadoop functionnalities of spark needs the winutils.exe file to be present on windows
    val sep = File.separator
    val current = new File(".").getAbsolutePath.replace(s"$sep.", "")
    System.setProperty("hadoop.home.dir", s"$current${sep}src${sep}test${sep}resources$sep")
  }

}
