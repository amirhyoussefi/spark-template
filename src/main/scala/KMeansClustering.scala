import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.util.Vector


/**
 * See http://en.wikipedia.org/wiki/K-means_clustering
 */
object KMeansClustering {

  private def parseArray(line: String): Array[Double] = {
    val words = line.split(" ")
    words.map(x => x.toDouble)
  }

  def main(args: Array[String]) {
    val sc = Helper.getSparkContext(args)

    // read data
    val file = "./test-data/kmeans_test_data.txt"
    val lines = sc.textFile(file)
    val data = lines.map(l => parseArray(l)).cache()

    // create KMeans model using the spark-mllib module
    val kmeans = new KMeans().setK(3)
    val model = kmeans.run(data)

    // let's print the cluster centers
    val centers = model.clusterCenters
    centers.zipWithIndex.foreach {
      case (c, i) => {
        val v = new Vector(c)
        println(s"Cluster $i = $v")
      }
    }

    // let's do some prediction with our model
    val point = new Vector(Array(7.0, 4.0))
    val c = model.predict(point.elements)
    println(s"Cluster for point $point = $c")
  }

}
