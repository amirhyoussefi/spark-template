package com.damirvandic.classification.text

import example.Helper
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.broadcast.Broadcast
import scala.collection.{mutable, Map}
import scala.io.Source
import java.io.File

class Index(sc: SparkContext, hierarchyPath: String) {
  val lines = Source.fromFile(new File(hierarchyPath)).getLines()

  val invertedIndex = lines.map(line => {
    line.split("[ ]+") match {
      case Array(parent, child) => {
        (child.trim.toInt, parent.trim.toInt)
      }
    }
  }).toList.groupBy(_._1).mapValues(vals => Set(vals.map(_._2): _*))

  val index = lines.map(line => {
    line.split("[ ]+") match {
      case Array(parent, child) => {
        (parent.trim.toInt, child.trim.toInt)
      }
    }
  }).toList.groupBy(_._1).mapValues(vals => Set(vals.map(_._2): _*))

  def reverseLookup(label: Int) = invertedIndex(label)

  def reverseContains(label: Int): Boolean = invertedIndex.contains(label)

  def lookup(label: Int) = index(label)

  def contains(label: Int): Boolean = index.contains(label)
}

object SparkTreeConstruction {

  private def addLabels(label: Int, index: Index, labels: mutable.Set[Int]): Unit = {
    labels.add(label)
    if (index.reverseContains(label)) {
      for (parent <- index.reverseLookup(label)) {
        if (!labels.contains(parent)) {
          addLabels(parent, index, labels)
        }
      }
    }
  }

  def main(args: Array[String]) {

    val hierarchyPath = "/Users/damirv/Downloads/large-scale text classification/hierarchy.txt"
    val documentsPath = "/Users/damirv/Downloads/large-scale text classification/train.csv"
    //    val hierarchyPath = "./text-classification/hierarchy.txt"
    //    val documentsPath = "./text-classification/train.csv"

    val master = "local[8]"
    val JARS = Seq.empty
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName("Word Count Job")
      .set("spark.default.parallelism", "8")
      .setJars(JARS)
    val sc = new SparkContext(conf)

    val indexBC = sc.broadcast(new Index(sc, hierarchyPath))

    val filteredLines = sc.textFile(documentsPath).sample(false, .001, 42).filter(line => !line.startsWith("Data") && line.indexOf(',') != -1)
    //    val filteredLines = sc.textFile(documentsPath).filter(line => !line.startsWith("Data") && line.indexOf(',') != -1)
    val docsToLabels = filteredLines.flatMap(line => {
      val startDocRepr = findStartDocRepr(line)
      val parts = line.substring(0, startDocRepr).split(",")
      val docID = parts(0).trim.toInt
      for (label <- parts.drop(1)) yield (docID, label.trim.toInt)
    })


    val labelsToDocs = docsToLabels.flatMap {
      case (doc, label) => {
        val invertedIndex = indexBC.value
        val labels = scala.collection.mutable.Set.empty[Int]
        addLabels(label, invertedIndex, labels)
        labels.map((_, doc))
      }
    }.combineByKey(
        (v: Int) => Set(v),
        (s: Set[Int], v: Int) => s + v,
        (c1: Set[Int], c2: Set[Int]) => c1.union(c2), 8)
    //          .groupByKey.map(pair => (pair._1, Set(pair._2: _*)))


    //combineByKey(v => Set(v), ???, ???)
    labelsToDocs take 100 foreach (println)

    //    Thread.sleep(1000 * 3600)
  }

  private def findStartDocRepr(line: String) = {
    var i = line.indexOf(':')
    while (i >= 0 && line.charAt(i) != ' ') {
      i -= 1
    }
    i + 1
  }
}
