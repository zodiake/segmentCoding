package com.nielsen.packsize

object PacksizeCoding {
  val plusString = "\\d+\\+".r
  val crossString = "\\d+\\*".r

  def parseDouble(s: List[Char]) = try {
    val temp = s.mkString
    if (temp.indexOf("*") > 0)
      Some(temp.split("\\*").map(_.toDouble).product)
    else if (temp.indexOf("X") > 0)
      Some(temp.split("X").map(_.toDouble).product)
    else if (temp.indexOf("+") > 0) {
      Some(temp.split("\\+").map(_.toDouble).sum)
    } else
      Some(s.mkString.toDouble)
  } catch {
    case _: Throwable => None
  }

  @annotation.tailrec
  def numberExtract(desc: String, pack: String, result: List[Double] = Nil): List[Double] = {
    if (desc.indexOf(pack) > -1) {
      def goLeft(s: String, result: List[Char] = Nil): Option[Double] = {
        if (s != "" && (s.last.isDigit || s.last == '.' || s.last.toUpper == 'X' || s.last == '+' || s.last == '*')) {
          goLeft(s.init, s.last :: result)
        } else {
          result match {
            case Nil => None
            case _   => parseDouble(result)
          }
        }
      }

      def goRight(s: String, result: List[Char] = Nil): Option[String] = {
        s.toList.map(dsc2Bsc) match {
          case h :: t if (h == '*' || h == '+' || h.isDigit) => goRight(s.drop(1), h :: result)
          case _ => if (result == Nil) None else Some(result.mkString)
        }
      }
      val left = desc.take(desc.indexOf(pack))
      val right = desc.drop(desc.indexOf(pack) + pack.size)
      val basePrice = goLeft(left)
      val number = goRight(right)
      val packSize = (basePrice, number) match {
        case (Some(a), Some(b)) if (plusString.pattern.matcher(b).matches) => packsizeTransform(pack, a)._2 + (b.substring(0, b.length() - 1).toDouble)
        case (Some(a), Some(b)) if (crossString.pattern.matcher(b).matches) => packsizeTransform(pack, a)._2 * (b.substring(0, b.length() - 1).toDouble)
        case (Some(a), None) => a
        case (_, _) => 0
      }
      numberExtract(right, transformPack(pack), packSize :: result)
    } else {
      result
    }
  }

  def transformPack(pack: String): String = {
    if (pack == "KG") {
      "G"
    } else if (pack == "L") {
      "ML"
    } else if (pack == "OZ") {
      "G"
    } else if (pack == "J") {
      "G"
    } else
      pack
  }

  def packsizeTransform(pack: String, size: Double): (String, Double) = {
    if (pack == "KG") {
      ("G", 1000 * size)
    } else if (pack == "L") {
      ("ML", 1000 * size)
    } else if (pack == "OZ") {
      ("G", 28.3495231 * size)
    } else if (pack == "J") {
      ("G", 500 * size)
    } else
      (pack, size)
  }

  private def dsc2Bsc(input: Char): Char =
    {
      var i = input;
      if (i == '\u3000') {
        i = ' ';
      } else if (i > '\uFF00' && i < '\uFF5F') {
        i = (i - 65248).toChar;
      }
      return i;

    }

  def getPackSize(description: String, packSize: String): Option[Double] = {
    try { Some(numberExtract(description, packSize).max) } catch { case _: Throwable => None }
  }
}