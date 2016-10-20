package com.github.andfanilo.template.apps

import com.github.andfanilo.template.AbstractApp
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingApp extends AbstractApp {

  override def execute(args: Array[String]) {
    val ssc = new StreamingContext(sparkSession.sparkContext, Seconds(1))

    val filePath = "file:///C:/tmp/"
    val lines = ssc.textFileStream(filePath)
    val words = lines.flatMap(_.split(","))
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }
}