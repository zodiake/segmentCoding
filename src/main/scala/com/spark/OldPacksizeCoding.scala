package com.spark

object OldPacksizeCoding {
  def main(args: Array[String]): Unit = {

    def PacksizeCoding(itemdesc: String, packname: String, packlist: List[Float]): List[Float] = {
      if (itemdesc.indexOf(packname) >= 0) {
        println(itemdesc)
        val packpos = itemdesc.indexOf(packname)
        val leftString = itemdesc.dropRight(itemdesc.size - packpos)
        val c = List()
        var rightPack = 1.toFloat
        val leftPack = toLeftCoding(leftString, c)
        val rightString = itemdesc.drop(packpos + packname.length())
        val result = PacksizeCoding(rightString, packname, packlist)
        if (!rightString.isEmpty()) {
          if (rightString.apply(0) == '*' || rightString.apply(0).toUpper == 'X') {
            rightPack = toRightCoding(rightString.drop(1), c)
          } else { rightPack = 1 }
          ((leftPack * rightPack) :: packlist) ++ result
        } else { (leftPack :: packlist) ++ result }
      } else List()
    }

    def toLeftCoding(x: String, y: List[Char]): Float = {
      var num = 0.toFloat
      try {
        num = y.mkString.toFloat
      } catch {
        case e: NumberFormatException => num = 0.toFloat
      }

      if (!x.isEmpty()) {
        if (x.reverse.head.isDigit) {
          toLeftCoding(x.reverse.drop(1).reverse, DSC2BSC(x.reverse.head) :: y)
        } else if (x.reverse.head == '.' && !y.isEmpty) {
          toLeftCoding(x.reverse.drop(1).reverse, DSC2BSC(x.reverse.head) :: y)
        } else if ((x.reverse.head == '*' || x.reverse.head.toUpper == 'X') && !y.isEmpty) {
          if (toLeftCoding(x.reverse.drop(1).reverse, List()) != 0) {
            toLeftCoding(x.reverse.drop(1).reverse, List()) * num
          } else {
            num
          }
        } else if (x.reverse.head == '+' && !y.isEmpty) {
          if (toLeftCoding(x.reverse.drop(1).reverse, List()) != 0) {
            toLeftCoding(x.reverse.drop(1).reverse, List()) + num
          } else {
            num
          }
        } else if (!y.isEmpty) {
          num
        } else 0
      } else 0
    }

    def toRightCoding(x: String, y: List[Char]): Float = {
      var temp = x
      var num = 0.toFloat
      try {
        num = y.mkString.toFloat
      } catch {
        case e: NumberFormatException => num = 0.toFloat
      }
      if (!temp.isEmpty()) {
        if (temp.head.isDigit && !temp.drop(1).isEmpty()) {
          toRightCoding(temp.drop(1), (DSC2BSC(temp.head) :: y.reverse).reverse)
        } else if (temp.head == '.' && !temp.drop(1).isEmpty()) {
          toRightCoding(temp.drop(1), (DSC2BSC(temp.head) :: y.reverse).reverse)
        } else if (temp.head.isDigit && temp.drop(1).isEmpty()) {
          (DSC2BSC(temp.head) :: y.reverse).reverse.mkString.toFloat
        } else if (!y.isEmpty) {
          num
        } else 1
      } else 1
    }

    //全角转半角
    def DSC2BSC(input: Char): Char = {
      var i = input;
      if (i == '\u3000') {
        i = ' ';
      } else if (i > '\uFF00' && i < '\uFF5F') {
        i = (i - 65248).toChar;
      }
      return i;

    }

    val l = PacksizeCoding("QUAKER/桂格【桂格旗舰店】 即食燕麦片1000g*2罐+燕麦乳香蕉味250ml*6瓶生产许可证编号:QS4419 0701 0416||厂名:东莞市日隆食品有限公司||厂址:东莞市虎门镇南栅六区民昌路七巷2号||厂家联系方式:8008201718||保质期:540 天||净含量:1000g||包装方式:包装||品牌:QUAKER/桂格||系列:燕麦乳250ml*6+1000g*2||产地:中国大陆||省份:其他", "g", List())
    println(l)
  }
}