package com.spark

object GradienDescent {
  def main(args: Array[String]): Unit = {
    def step(loop: Int, source: List[List[Float]], learningRate: Float, o0: Float = 0, o1: Float = 0): (Float, Float) = {
      def costFunction(o0: Float, o1: Float, index: Int, matrix: List[Float]): Float = {
        (o0 * matrix(1) + o1 - matrix.last) * matrix(index)
      }
      val m = source.length.toFloat
      var r0 = 0f;
      var r1 = 0f;
      for {
        s <- source
      } {
        r0 += (s(1) * o1 + o0 - s(2))
        r1 += (s(1) * o1 + o0 - s(2)) * s(1)
      }
      val c0 = o0 - learningRate / m * r0
      val c1 = o1 - learningRate / m * r1

      if (loop - 1 == 0)
        (c0, c1)
      else
        step(loop - 1, source, learningRate, c0, c1)
    }
    println(step(1000, List(List(1, 1, 1), List(1, 2, 2), List(1, 3, 3), List(1, 4, 4)), 0.1f, 0, 0))
  }
}