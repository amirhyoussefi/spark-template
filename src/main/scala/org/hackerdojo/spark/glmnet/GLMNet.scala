package org.hackerdojo.spark.glmnet

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.hackerdojo.spark.Helper

object GLMNet {
  def main(args: Array[String]) {
    val sc = Helper.getSparkContext(args)
    val sourceFile = ""
    val runner = GLMNet(sc, sourceFile)
  }
}

case class GLMNet(val sc: SparkContext, val sourceFile: String) {

}
