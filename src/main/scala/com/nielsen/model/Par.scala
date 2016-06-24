package com.nielsen.model

sealed trait Par {
  val a = "[a-zA-Z]+&?[a-zA-Z]+".r
  val b = "([a-zA-Z]+\\W+)|(\\W+)|(\\W+[a-zA-Z]+)".r

  def extractEnglish(s: String) = {
    a.findAllIn(s).matchData.map(i => (i.matched.toUpperCase(), i.matched)).toMap
  }

  def keyWordContainChinese(s: String) = {
    b.pattern.matcher(s).matches()
  }

  def parse(desc: String): Int
}

case class SimplePar(s: String) extends Par {
  def parseChinese(desc: String): Int = {
    desc.indexOf(s.toUpperCase())
  }

  def parseEnglish(desc: String): Int = {
    val english = extractEnglish(desc)
    if (english.keys.toList.contains(s.toUpperCase())) {
      desc.indexOf(english(s.toUpperCase))
    } else
      -1
  }

  def parse(desc: String): Int = {
    if (keyWordContainChinese(s))
      parseChinese(desc)
    else
      parseEnglish(desc)
  }
}

case class NotMatchSimplePar(s: String) extends Par {
  def parseChinese(desc: String): Int = {
    desc.indexOf(s.toUpperCase())
  }

  def parseEnglish(desc: String): Int = {
    val english = extractEnglish(desc)
    if (english.keys.toList.contains(s.toUpperCase)) {
      desc.indexOf(english(s.toUpperCase))
    } else
      -1
  }

  def parse(desc: String): Int = {
    if (keyWordContainChinese(s))
      parseChinese(desc)
    else
      parseEnglish(desc)
  }
}

case class BothPar(s: (String, String)) extends Par {
  def parseChinese(desc: String): Int =
    Math.min(desc.indexOf(s._1.toUpperCase), desc.indexOf(s._2.toUpperCase))

  def parseEnglish(desc: String): Int = {
    val english = extractEnglish(desc)
    if (english.keys.toList.contains(s._1.toUpperCase) && english.keys.toList.contains(s._2.toUpperCase))
      Math.min(desc.indexOf(english(s._1.toUpperCase)), desc.indexOf(extractEnglish(desc)(s._2.toUpperCase)))
    else
      -1
  }

  def parse(desc: String): Int = {
    lazy val english = extractEnglish(desc)
    (keyWordContainChinese(s._1), keyWordContainChinese(s._2)) match {
      case (true, true) => parseChinese(desc)
      case (false, false) => parseEnglish(desc)
      case (true, false) => if (english.contains(s._1)) Math.min(desc.indexOf(s._1), desc.indexOf(extractEnglish(desc)(s._2.toUpperCase))) else -1
      case (false, true) => if (english.contains(s._2)) Math.min(desc.indexOf(extractEnglish(desc)(s._1.toUpperCase)), desc.indexOf(s._2)) else -1
    }
  }
}

case class OneExpectPar(mustMatch: String, mustNotMatch: List[String]) extends Par {
  def parseChinese(desc: String, keyWord: String): Int = {
    desc.indexOf(keyWord.toUpperCase)
  }

  def parseEnglish(desc: String, keyWord: String): Int = {
    val english = extractEnglish(desc)
    if (english.keys.toList.contains(keyWord.toUpperCase)) {
      desc.indexOf(english(keyWord.toUpperCase))
    } else
      -1
  }

  def parse(desc: String): Int = {
    val index = indexOfMustMatch(desc)
    if (index == -1)
      -1
    else {
      if (containMustNotMatch(desc, mustNotMatch))
        -1
      else
        index
    }
  }

  def indexOfMustMatch(desc: String): Int = {
    if (keyWordContainChinese(mustMatch))
      parseChinese(desc, mustMatch)
    else
      parseEnglish(desc, mustMatch)
  }

  def containMustNotMatch(desc: String, list: List[String]): Boolean = {
    list match {
      case Nil => false
      case h :: t => {
        lazy val chineseIndex = parseChinese(desc, h)
        lazy val englishIndex = parseEnglish(desc, h)
        if (keyWordContainChinese(h))
          if (chineseIndex > -1) true else containMustNotMatch(desc, t)
        else {
          if (englishIndex > -1) true else containMustNotMatch(desc, t)
        }
      }
    }
  }
}

object Par {

  implicit class Regex(sc: StringContext) {
    def r = new util.matching.Regex(sc.parts.mkString, sc.parts.tail.map(_ => "x"): _*)
  }

  def parser(t: String): List[Par] = {
    t.split(";").map(i =>
      i match {
        case r"(.*)${first}/\{(.*)${second}\}" => OneExpectPar(first, second.split("\\$").toList)
        case r"(.*)${first}/(.*)${second}" => BothPar((first, second))
        case r"\{(.*)${first}\}" => NotMatchSimplePar(first)
        case _ => SimplePar(i)
      }).toList
  }

  def parse(idAndKeyWord: IdAndKeyWord)(desc: String): SegIdWithIndexAndSegName = {
    val keyWordList = parser(idAndKeyWord.keyWord)
    def go(list: List[Par], result: List[Int] = List()): List[Int] = {
      list match {
        case Nil => result
        case h :: t => h match {
          case _: NotMatchSimplePar => if (h.parse(desc) > -1) Nil else go(t, result)
          case _ => if (h.parse(desc) > -1) go(t, h.parse(desc) :: result) else go(t, result)
        }
      }
    }
    val indexes = go(keyWordList)
    SegIdWithIndexAndSegName(idAndKeyWord.id, idAndKeyWord.keyWord, indexes, idAndKeyWord.parentNo)
  }
}

case class SegIdWithIndexAndSegName(segmentId: String, segmentName: String, index: List[Int], parentNo: String)

case class IdAndKeyWord(id: String, keyWord: String, parentNo: String="")