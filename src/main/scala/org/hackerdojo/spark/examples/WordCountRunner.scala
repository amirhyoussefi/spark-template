package org.hackerdojo.spark

import org.apache.spark.SparkContext._

object WordCountRunner {

  def main(args: Array[String]) {
    val sc = Helper.getSparkContext(args)

    //    val lines = sc.textFile("./test-data/some_text.txt", 3)
    val wordCountPairs = sc.textFile("/Users/damirv/Downloads/docword.nytimes.txt")
      .map(l => {
      val splitted = l.split(" ")
      (splitted(1).trim.toInt, splitted(2).trim.toInt)
    })

    val res = wordCountPairs.reduceByKey((x:Int, y:Int) => {
      x + y
    }, 10).collect()

    res.take(200).foreach(println(_))

    // print the counts
    //    counts.take(200).foreach(println(_))
    Thread.sleep(1000 * 3600)
  }
}
