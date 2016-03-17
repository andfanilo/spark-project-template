package com.github.andfanilo.template.unit

import org.scalatest.{DoNotDiscover, Suites}

/**
  * Entry point for tests
  */
@DoNotDiscover
class UnitSuite extends Suites(
  new SampleUnitTest
)
