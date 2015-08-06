package com.github.fandetwa.template.spark

import com.github.fandetwa.template.spark.RDDImplicits.RichRDD
import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{DoNotDiscover, FreeSpec, Matchers}

@DoNotDiscover
class RDDImplicitsTest extends FreeSpec with Matchers with SharedSparkContext {

  "Should reduce number of partitions of an rdd" in {
    val rdd = sc.parallelize(1 to 100).repartition(5)

    rdd.partitions.length shouldBe 5

    rdd.reduceToNbPartitions(0).partitions.length shouldBe 5
    rdd.reduceToNbPartitions(3).partitions.length shouldBe 3
    rdd.reduceToNbPartitions(5).partitions.length shouldBe 5
    rdd.reduceToNbPartitions(7).partitions.length shouldBe 5

    sc.stop()
  }
}
