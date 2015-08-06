package com.github.fandetwa.template.spark

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * All implicit classes for enriching RDD functionality
 */
object RDDImplicits {

  implicit class RichRDD[T: ClassTag](rdd: RDD[T]) {

    /**
     * Repartition RDD to number of partitions
     * @param nbOutputPartitions number of partitions for output rdd
     * @return if nbOutputPartitions =< 0 don't do anything
     *         else return rdd repartitioned with selected number of output partitions
     */
    def reduceToNbPartitions(nbOutputPartitions: Int = -1) = {
      nbOutputPartitions match {
        case x: Int if x <= 0 => rdd
        case x: Int if x < rdd.partitions.size => rdd.coalesce(nbOutputPartitions, shuffle = false)
        case _ => rdd
      }
    }
  }

}
