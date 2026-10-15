package org.apache.spark.ml.tree

import org.apache.spark.ml.linalg.Vector

object DecisionPath {
  private def attackShare(node: Node): Double = {
    val counts = node.impurityStats.stats
    if (counts.sum == 0.0) 0.0 else counts(1) / counts.sum
  }

  def contributions(root: Node, features: Vector, featureCount: Int): Array[Double] = {
    val credit = new Array[Double](featureCount)
    var node = root
    while (node.isInstanceOf[InternalNode]) {
      val inner = node.asInstanceOf[InternalNode]
      val next = if (inner.split.shouldGoLeft(features)) inner.leftChild else inner.rightChild
      credit(inner.split.featureIndex) += attackShare(next) - attackShare(node)
      node = next
    }
    credit
  }
}
