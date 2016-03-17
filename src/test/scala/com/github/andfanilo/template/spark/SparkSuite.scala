package com.github.andfanilo.template.spark

import org.scalatest.{DoNotDiscover, Suites}

/**
  * Entry point for tests
  */
@DoNotDiscover
class SparkSuite extends Suites(
  new RDDImplicitsTest
)
