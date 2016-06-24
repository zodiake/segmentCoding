package test

import org.scalatest.FunSuite
import com.nielsen.model.BothPar

class BothParserSuit extends FunSuite {
  val desc = "申通包邮 法国 MM豆MMS巧克力豆机M&amp;MS 圣诞老人糖果机 全新款式"

  test("english,english") {
    val parser = BothPar("mm", "mms")
    assert(parser.parse(desc) == 8)
  }

  test("englishchinese englishchinese") {
    val parser = BothPar("mm豆", "mms巧克")
    assert(parser.parse(desc) == 8)
  }

  test("chinese chinese") {
    val parser = BothPar("豆", "巧克")
    assert(parser.parse(desc) == 10)
  }

  test("chineseenglish chinese") {
    val parser = BothPar("mm豆", "巧克")
    assert(parser.parse(desc) == 8)
  }

  test("not exist") {
    val parser = BothPar("asdf", "克巧")
    assert(parser.parse(desc) == -1)
  }
}