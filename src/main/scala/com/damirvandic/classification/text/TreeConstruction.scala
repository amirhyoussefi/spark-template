package com.damirvandic.classification.text

import scala.io.Source
import java.io.File
import scala.collection.mutable.{Set => MutableSet}
import scala.collection.mutable.{Map => MutableMap}
import scala.collection.mutable

case class MutableDocument(id: Int) {
  val features: MutableMap[Int, Int] = MutableMap.empty

  override def hashCode(): Int = id

  override def equals(obj: scala.Any): Boolean = canEqual(obj) && obj.asInstanceOf[MutableDocument].id == id
}

case class MutableLabel(id: Int,
                        documents: MutableSet[MutableDocument] = MutableSet.empty,
                        children: MutableSet[MutableLabel] = MutableSet.empty) {

  override def hashCode(): Int = id

  override def equals(obj: scala.Any): Boolean = canEqual(obj) && obj.asInstanceOf[MutableLabel].id == id

  override def toString: String = s"N($id)"
}

class MutableTree {
  val rootLabels = MutableSet.empty[MutableLabel]
  val childLabels = MutableSet.empty[MutableLabel]

  val docs = MutableMap.empty[Int, MutableDocument]
  val labels = MutableMap.empty[Int, MutableLabel]

  def addEdge(parentLabelID: Int, childLabelID: Int) = {
    val parentLabel = getOrCreateLabel(parentLabelID)
    val childLabel = getOrCreateLabel(childLabelID)
    parentLabel.children.add(childLabel)

    // keep track of root labels
    rootLabels.remove(childLabel)
    childLabels.add(childLabel)
    if (!childLabels.contains(parentLabel)) {
      rootLabels.add(parentLabel)
    }
  }

  def addLabel(docID: Int, categoryID: Int) = {
    val doc = getOrCreateDocument(docID)
    val category = getOrCreateLabel(categoryID)
    category.documents.add(doc)
  }

  def addFeature(docID: Int, feature: Int, count: Int) = {
    val doc = getOrCreateDocument(docID)
    doc.features(feature) = count
  }

  def getStringRepr = {
    def get(node: MutableLabel, indent: Int): String = {
      val builder = new mutable.StringBuilder()
      builder.append(" " * indent)
      builder.append(node.id.toString)
      for (child <- node.children) {
        builder.append('\n')
        builder.append(get(child, indent + 2))
      }
      builder.toString
    }
    val builder = new mutable.StringBuilder
    builder.append("[\n")
    for (root <- rootLabels) {
      builder.append(get(root, 2))
      builder.append('\n')
    }
    builder.append("]")
    builder.toString
  }

  override def toString: String = s"Tree[${rootLabels.size} roots]"

  private def getOrCreateLabel(categoryID: Int) = {
    if (labels.contains(categoryID)) {labels(categoryID)} else {val tmp = MutableLabel(categoryID); labels(categoryID) = tmp; tmp}
  }

  private def getOrCreateDocument(docID: Int) = {
    if (docs.contains(docID)) {docs(docID)} else {val tmp = MutableDocument(docID); docs(docID) = tmp; tmp}
  }
}

object TreeConstructor {

  private def addEdges(hierarchy: Iterator[String], tree: MutableTree) {
    for (line <- hierarchy) {
      val parts = line.split(" ")
      tree.addEdge(parts(0).trim.toInt, parts(1).trim.toInt)
    }
  }

  private def addLabels(docID: Int, categorys: String, tree: MutableTree) {

    for (category <- categorys.split(",")) {
      tree.addLabel(docID, category.trim.toInt)
    }
  }

  private def findStartDocRepr(line: String) = {
    var i = line.indexOf(':')
    while (i >= 0 && line.charAt(i) != ' ') {
      i -= 1
    }
    i + 1
  }

  private def addDocument(docID: Int, docRepr: String, tree: MutableTree) = {
    for (pair <- docRepr.split("[ ]+")) {
      val parts = pair.split(":")
      tree.addFeature(docID, parts(0).trim.toInt, parts(1).trim.toInt)
    }
  }

  private def addDocsToLeafs(documents: Iterator[String], tree: MutableTree) = {
    var i = 0
    for (line <- documents) {
      i += 1
      if (i % 10000 == 0) {
        println(i)
      }
      val firstCommaIndex = line.indexOf(',')
      if (firstCommaIndex >= 0) {
        val docID = line.substring(0, firstCommaIndex).trim.toInt
        val startIndexDocRepr = findStartDocRepr(line)

        val docRepr = line.substring(startIndexDocRepr, line.length)
        addDocument(docID, docRepr, tree)

        val categorys = line.substring(firstCommaIndex + 1, startIndexDocRepr)
        addLabels(docID, categorys, tree)
      }
    }
  }

  def constructTree(hierarchyPath: String, documentsPath: String): MutableTree = {
    val hierarchy = Source.fromFile(new File(hierarchyPath)).getLines()
    val documents = Source.fromFile(new File(documentsPath)).getLines()
    documents.next() // skip header

    val tree = new MutableTree
    addEdges(hierarchy, tree)
    addDocsToLeafs(documents, tree)
    // push docs throughout the whole tree
    tree
  }
}

object TreeConstruction {
  def main(args: Array[String]) {
    val start = System.nanoTime
    //    val hierarchyPath = "./text-classification/hierarchy.txt"
    //    val documentsPath = "./text-classification/train.csv"
    val hierarchyPath = "/Users/damirv/Downloads/large-scale text classification/hierarchy.txt"
    val documentsPath = "/Users/damirv/Downloads/large-scale text classification/train.csv"
    val tree = TreeConstructor.constructTree(hierarchyPath, documentsPath)
    println(tree)
    println(s"It took ${(System.nanoTime - start) / 1E9} seconds")
  }
}
