package com.nielsen.model

/*
  @param segmentId
  @param par splitted keyword
  @param index
  @param parentNo
 */
case class SegIdWithIndexAndSegName(segmentId: String, par: String, index: Int, parentNo: String)

case class IdAndKeyWordAndParentNo(id: String, keyWord: String, parentNo: String = "")

abstract class Par(source: String) {
  val a = "[a-zA-Z]+&?[a-zA-Z]+".r
  val b = "([a-zA-Z]+\\W+)|(\\W+)|(\\W+[a-zA-Z]+)".r

  def extractEnglish(s: String) = {
    a.findAllIn(s).matchData.map(i => (i.matched.toUpperCase(), i.matched)).toMap
  }

  def keyWordContainChinese(s: String) = {
    s.exists(Character.UnicodeScript.of(_) == Character.UnicodeScript.HAN)
  }

  def parse(desc: String): (String, Int)
}

case class SimplePar(s: String) extends Par(s) {
  def parseChinese(desc: String): (String, Int) = {
    (s, desc.indexOf(s.toUpperCase()))
  }

  def parseEnglish(desc: String): (String, Int) = {
    val english = extractEnglish(desc)
    if (english.keys.toList.contains(s.toUpperCase())) {
      (s, desc.indexOf(english(s.toUpperCase)))
    } else
      (s, -1)
  }

  override def parse(desc: String): (String, Int) = {
    if (keyWordContainChinese(s))
      parseChinese(desc)
    else
      parseEnglish(desc)
  }
}

case class NotMatchSimplePar(source: String, s: String) extends Par(source) {
  def parseChinese(desc: String): (String, Int) = {
    (source, desc.indexOf(s.toUpperCase()))
  }

  def parseEnglish(desc: String): (String, Int) = {
    val english = extractEnglish(desc)
    if (english.keys.toList.contains(s.toUpperCase)) {
      (source, desc.indexOf(english(s.toUpperCase)))
    } else
      (source, -1)
  }

  override def parse(desc: String): (String, Int) = {
    if (keyWordContainChinese(s))
      parseChinese(desc)
    else
      parseEnglish(desc)
  }
}

case class BothPar(source: String, s: (String, String)) extends Par(source) {
  def parseChinese(desc: String): (String, Int) =
    (source, Math.min(desc.indexOf(s._1.toUpperCase), desc.indexOf(s._2.toUpperCase)))

  def parseEnglish(desc: String): (String, Int) = {
    val english = extractEnglish(desc)
    if (english.keys.toList.contains(s._1.toUpperCase) && english.keys.toList.contains(s._2.toUpperCase))
      (source, Math.min(desc.indexOf(english(s._1.toUpperCase)), desc.indexOf(extractEnglish(desc)(s._2.toUpperCase))))
    else
      (source, -1)
  }

  override def parse(desc: String): (String, Int) = {
    lazy val english = extractEnglish(desc)
    (keyWordContainChinese(s._1), keyWordContainChinese(s._2)) match {
      case (true, true) => parseChinese(desc)
      case (false, false) => parseEnglish(desc)
      case (true, false) => if (english.contains(s._1)) (source, Math.min(desc.indexOf(s._1), desc.indexOf(extractEnglish(desc)(s._2.toUpperCase)))) else (source, -1)
      case (false, true) => if (english.contains(s._2)) (source, Math.min(desc.indexOf(extractEnglish(desc)(s._1.toUpperCase)), desc.indexOf(s._2))) else (source, -1)
    }
  }
}

case class OneExpectPar(source: String, mustMatch: String, mustNotMatch: List[String]) extends Par(source) {
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

  override def parse(desc: String): (String, Int) = {
    val index = indexOfMustMatch(desc)
    if (index == -1)
      (source, -1)
    else {
      if (containMustNotMatch(desc, mustNotMatch))
        (source, -1)
      else
        (source, index)
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

  def parse(idAndKeyWord: IdAndKeyWordAndParentNo)(desc: String): List[SegIdWithIndexAndSegName] = {
    def go(list: List[Par], result: List[(String, Int)] = Nil): List[(String, Int)] = {
      list match {
        case Nil => result
        case h :: t => h match {
          case _: NotMatchSimplePar => if (h.parse(desc)._2 > -1) Nil else go(t, result)
          case _ => if (h.parse(desc)._2 > -1) go(t, h.parse(desc) :: result) else go(t, result)
        }
      }
    }

    val list = idAndKeyWord.keyWord.split(";").map(i =>
      i match {
        case r"(.*)${first}/\{(.*)${second}\}" => OneExpectPar(i, first, second.split("\\$").toList)
        case r"(.*)${first}/(.*)${second}" => BothPar(i, (first, second))
        case r"\{(.*)${first}\}" => NotMatchSimplePar(i, first)
        case _ => SimplePar(i)
      }).toList

    go(list).map(i => SegIdWithIndexAndSegName(idAndKeyWord.id, i._1, i._2, idAndKeyWord.parentNo))
  }
}

