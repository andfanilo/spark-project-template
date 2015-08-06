package com.github.fandetwa.template.integration

import org.scalatest.{DoNotDiscover, FreeSpec, Matchers}

/**
  * Just a simple test class
  */
@DoNotDiscover
class SampleIntegrationTest extends FreeSpec with Matchers {

   "Given a value" - {

     val a = 5

     "should check that it is passed correctly to subtests" in {
       a + 3 should be(8)
     }
   }

 }
