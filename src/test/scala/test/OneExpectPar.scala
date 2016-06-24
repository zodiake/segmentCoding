package test

import org.scalatest.FunSuite

import com.nielsen.model._

class OneExpectPar extends FunSuite {
  val desc = "申通包邮 法国 MM豆MMS巧克力豆机M&amp;MS 圣诞老人糖果机 全新款式"

  test("not contain must not match ") {
    val parser = OneExpectPar("MM豆", List("巧克力"))
    assert(parser.parse(desc) == -1)
  }

  test("not contain mustNotMatch ") {
    val parser = OneExpectPar("MM豆", List("acc"))
    assert(parser.parse(desc) == 8)
  }

  test("not contain must match") {
    val parser = OneExpectPar("cc", List("acc"))
    assert(parser.parse(desc) == -1)
  }
}