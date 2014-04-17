package org.hackerdojo.spark

import org.apache.spark.SparkContext._

object WordCountRunner {

  def main(args: Array[String]) {
    val sc = Helper.getSparkContext(args)

    val lines = sc.textFile("./test-data/some_text.txt", 3)
    val wordCountPairs = lines.flatMap(l => {
      val splitted = l.split(" ")
      splitted.map(word => (word, 1))
    })

    // word counts
    val wordCounts = wordCountPairs.reduceByKey((x: Int, y: Int) => {
      x + y
    }, 10)

    // write out partitions as text files
    wordCounts.saveAsTextFile("./results")

    // print out first 10
    wordCounts.take(10).foreach(println(_))

    // block so we can look at http://localhost:4040
    Thread.sleep(1000 * 3600)
  }
}
