package test

import org.scalatest.FunSuite
import com.nielsen.model.Par
import com.nielsen.model.SimplePar

class SimpleParser extends FunSuite {
  val desc = "申通包邮 法国 MM豆MMS巧克力豆机M&amp;MS 圣诞老人糖果机 全新款式"
  val b = "([a-zA-Z]+\\W+)|(\\W+)|(\\W+[a-zA-Z]+)".r
  test("extract english from description") {
    val a = "(\\w+&\\w+)|(\\w+)".r
    def extractEnglish(s: String) = {
      a.findAllIn(s).matchData.map(i => (i.matched.toUpperCase(), i.matched)).toMap
    }
    val englishMap = extractEnglish(desc)
    assert(englishMap == Map("MM" -> "MM", "MMS" -> "MMS", "M&AMP" -> "M&amp", "MS" -> "MS"))
  }

  test("keyword contain chinese") {
    assert(b.pattern.matcher("MM豆").matches() == true)
    assert(b.pattern.matcher("MM").matches() == false)
    assert(b.pattern.matcher("巧克力").matches() == true)
  }

  test("simple parser parse desc with chinese English") {
    val simpleParser1 = SimplePar("MM豆")
    assert(simpleParser1.parse(desc) == 8)
  }

  test("simple parser parse desc with english") {
    val simpleParser = SimplePar("MM")
    assert(simpleParser.parse(desc) == 8)
  }

  test("with chinese") {
    val simpleParser = SimplePar("巧克力")
    assert(b.pattern.matcher("巧克力").matches() == true)
    assert(simpleParser.parse(desc) == 14)
  }
} 