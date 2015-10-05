package com.github.fandetwa.template.spark

import com.github.fandetwa.template.spark.RDDImplicits.RichRDD
import org.scalatest.DoNotDiscover

@DoNotDiscover
class RDDImplicitsTest extends SparkSuite {

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
