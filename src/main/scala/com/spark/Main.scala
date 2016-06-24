package com.spark

object Main {
  /*
    transaction-1: a,b,c
		transaction-2: a,b,d
	  transaction-3: b,c
	  transaction-4: b,c
   */
  def main(string: Array[String]) {
    def spLengthCoding(itemdesc: String, packname: String, packlist: List[Float]): List[Float] = {
      if (itemdesc.indexOf(packname) >= 0 || itemdesc.indexOf(packname.toUpperCase()) >= 0) {
        var packpos = itemdesc.indexOf(packname)
        if (packpos == -1) packpos = itemdesc.indexOf(packname.toUpperCase())
        val leftString = itemdesc.dropRight(itemdesc.size - packpos)
        val c = List()
        var rightPack = 1.toFloat
        val leftPack = toLeftCoding(leftString, c)
        val rightString = itemdesc.drop(packpos + packname.length())
        val result = spLengthCoding(rightString, packname, packlist)
        (leftPack :: packlist) ++ result
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
        } else if (!y.isEmpty) {
          num
        } else 0
      } else {
        num
      }
    }

    def DSC2BSC(input: Char): Char = {
      var i = input;
      if (i == '\u3000') {
        i = ' ';
      } else if (i > '\uFF00' && i < '\uFF5F') {
        i = (i - 65248).toChar;
      }
      return i;

    }

    println(spLengthCoding("22.5cm ;22.5CM;238MM", "MM", Nil))
    println(spLengthCoding("42cm;40CM;38CM", "MM", Nil))
  }
}