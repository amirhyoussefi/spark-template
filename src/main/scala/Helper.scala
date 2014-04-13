import org.apache.spark.SparkContext

object Helper {

  def getSparkContext(args: Array[String]) = {
    assert(args.length == 0 || args.length == 2)
    val cores = Runtime.getRuntime.availableProcessors()
    val master = if (args.length > 0) args(0) else s"local[$cores]"
    val JARS = if (args.length > 0) Seq(args(1)) else Seq.empty
    new SparkContext(master, "Word Count Job", System.getenv("SPARK_HOME"), JARS)
  }

}
