/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.examples

import java.util.Random
import scala.math.exp
import org.apache.spark.util.Vector
import org.apache.spark._

/**
 * Logistic regression based classification.
 */
object SparkLR {
  val N = 10000
  // Number of data points
  val D = 10
  // Numer of dimensions
  val R = 0.7
  // Scaling factor
  val ITERATIONS = 5
  val rand = new Random(42)

  case class DataPoint(x: Vector, y: Double)

  def generateData = {
    def generatePoint(i: Int) = {
      val y = if (i % 2 == 0) -1 else 1
      val x = Vector(D, _ => rand.nextGaussian + y * R)
      DataPoint(x, y)
    }
    Array.tabulate(N)(generatePoint)
  }

  def main(args: Array[String]) {
    val cores = Runtime.getRuntime.availableProcessors()

    val master = if (args.length > 0) args(0) else s"local[$cores]"
    val JARS = if (args.length > 0) Seq(args(1)) else Seq.empty
    val sc = new SparkContext(master, "Word Count Job", System.getenv("SPARK_HOME"), JARS)

    val numSlices = cores
    val points = sc.parallelize(generateData, numSlices).cache()

    // Initialize w to a random value
    var w = Vector(D, _ => 2 * rand.nextDouble - 1)
    println("Initial w: " + w)

    for (i <- 1 to ITERATIONS) {
      println("On iteration " + i)
      val temp = points.map {
        p =>
          (1 / (1 + exp(-p.y * (w dot p.x))) - 1) * p.y * p.x
      }
      val gradient = temp.reduce(_ + _)
      w -= gradient
    }

    println("Final w: " + w)
    System.exit(0)
  }
}
