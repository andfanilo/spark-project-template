package com.github.andfanilo.template

import com.github.andfanilo.template.spark.SparkSuite
import org.scalatest.DoNotDiscover

/**
 * Just a simple test class
 */
@DoNotDiscover
class SampleUnitTest extends SparkSuite {

  "Given an RDD of integers" - {

    // at this point of the code, SparkSuite hasn't yet initialised SparkContext, so let rdd creation be lazy
    lazy val rdd = sc.parallelize(Seq(1, 2, 3, 4, 5))

    "should sum all integers in a distributed way" in {
      rdd.reduce(_ + _) should equal(15)
    }
  }

}
