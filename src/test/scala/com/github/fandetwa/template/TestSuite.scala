package com.github.fandetwa.template

import java.io.File

import com.github.fandetwa.template.integration.SampleIntegrationTest
import com.github.fandetwa.template.unit.SampleUnitTest
import org.scalatest.Suites

/**
 * Entry point for tests
 */
object TestSuite {
  // Hadoop functionnalities of spark needs the winutils.exe file to be present on windows
  val sep = File.separator
  val current = new File(".").getAbsolutePath.replace(s"$sep.", "")
  System.setProperty("hadoop.home.dir", s"$current${sep}src${sep}test${sep}resources$sep")
}

class TestSuite extends Suites(
  new SampleUnitTest,
  new SampleIntegrationTest
)
