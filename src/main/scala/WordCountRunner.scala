import org.apache.spark.SparkContext._

object WordCountRunner {

  def main(args: Array[String]) {
    val sc = Helper.getSparkContext(args)

    // read the lines and distribute them
    val lines = sc.textFile("./test-data/some_text.txt")

    // convert lines to array of strings (representing words)
    val words = lines.flatMap(l => l.split("[ ]+"))

    // convert each word to a pair (word, 1)
    val wordPairs = words.map(w => (w, 1))

    // reduce by the key, i.e., the word, and sum their values (effectively counting)
    val counts = wordPairs.reduceByKey(_ + _).collect().sortBy(countPair => -countPair._2)

    // print the counts
    counts.foreach(println(_))
  }
}