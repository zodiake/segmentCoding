package test

import org.scalatest.FunSuite

class ScalaFeatureSuite extends FunSuite {
  test("empty array test") {
    assert(Array("1") != Array.empty[String])
  }
}