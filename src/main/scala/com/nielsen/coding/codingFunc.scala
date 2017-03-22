package com.nielsen.coding

import scala.util.control.Breaks._

class codingFunc extends java.io.Serializable {

  /**
    * KWLC keyword list coding
    * @param itemdesc target item description
    * @param wordlist key word list
    */

  def KWLC(itemdesc: String, wordlist: List[(String, String)], parentlist: List[(String, String)]): String = {
    val pattern = "[a-zA-z]+".r //英文正则
    val itemdesc_e = pattern.findAllIn(itemdesc).toList //英文 非字符串 为完全匹配英文单词
    val result = wordlist.map(x => (multifindDesc(itemdesc, x._2), x)) //遍历列表中所有的关键词
      .filter(_._1 >= 0) //去掉未匹配上的词
      .map(x => (x._1, x._2, parentlist.filter(y => y._1 == x._2._1).head._2)) //增加parentid
    val p = result.map(_._3).filter(_ != "-").distinct //取出distinct的parentid  ---?????
    val n = result.filter(x => p.exists(y => y == x._2._1)).map(_._2._2) //选出parentid不为“-”的-- parent id brand key worlds
    //result.foreach(println)
    //val re1 = result.filter(x => x._3 != x._2._1 ||(x._3 == x._2._1 && result.count(y => y._3 == x._3)==1))
    val re1 = result.filter(x => !n.exists(y => y == x._2._2)) //去掉存在parentid的
    //出现一个词包含另一个词的情况时,取长的词
    var result_f = re1.filter(x => re1.filter(y =>
      y._2._2.indexOf(x._2._2) >= 0 &&
        y._2._2.size != x._2._2.size).isEmpty)
    //result_f.filter(x => result_f.filter(y => y._2._2.indexOf(x._2._2) >= 0 && y._2._2.size != x._2._2.size).isEmpty ).foreach(println)
    // 判断是否存在英文品牌被找到
    //re1.foreach(println)
    if (!re1.map(x => findEng(x._2._2)).filter(_ != "").isEmpty) {
      result_f = result_f.filter(x => ! (itemdesc_e.indexOf(x._2._2) < 0 && findEng(x._2._2) != "" )) //按照完整的英文单词筛选
    }
    //result_f.foreach(println)
    if (!result_f.isEmpty) {
      //result_f.foreach(println)
      result_f.groupBy(_._1) //把相同位置的词合并
        .toList //转成list
        .sortBy(_._1) //按照词在描述中的位置从小到大排序
        .head //取第一个匹配上的词
        ._2 //取词的组合
        .sortBy(-_._2._2.size) //按照组合中的词长倒序排序
        .head._2._1 //取词长最大的词
    } else
      return ""
  }

  /**
    * MKWLC multi keyword list coding
    * @param itemdesc target item description
    * @param multiwordlist multi key word list
    */

  def MKWLC(itemdesc: String, multiwordlist: List[List[(String, String)]]): String = {
    try {
      val result = KWLC(itemdesc, multiwordlist.head, multiwordlist.reverse.head) //按顺序先算
      if (multiwordlist.size == 2) //判断是否最后一个列表
        return result
      else if (result != "") //结果不为空，返回
        return result
      else
        MKWLC(itemdesc, multiwordlist.tail) //递归 -- tail -- Selects all elements except the first one.
    } catch {
      case ex: Exception => ex.toString()
    }

  }

  /**
    * findDesc find desc split by "/"
    * @param itemdesc target item description
    * @param targetdesc multi desc
    */
  /*
   * spilt by "/" -- every str can be found in itemdesc -- return the min index
   *              -- else -- return -- -1
   *  targetdesc has no "/"  -- return the index of the targetdesc in itemdesc.
   */
  /*private def findDesc(itemdesc: String, targetdesc: String): Int = {
    if (targetdesc.indexOf("/") >= 0) { //如果存在斜杠分割的字符串则逐个寻找
      val targetlist = targetdesc.split("/").toList.map(x => itemdesc.indexOf(x))
      if (targetlist.min >= 0) { //如果每个分割的字符串都能找到则返回最前面的位置
        targetlist.min
      } else
        -1
    } else
      itemdesc.indexOf(targetdesc) //没有斜杠的字符串按正常方式查找
  }*/

  private def findDesc(itemdesc: String, targetdesc: String): Int = {
    val pattern = "[a-zA-z]+".r //英文正则
    val itemdesc_e = pattern.findAllIn(itemdesc).toList.map(_.toUpperCase()) //英文 非字符串 为完全匹配英文单词
    // val itemdesc_e_1 = pattern.findAllIn(itemdesc).toList.map(x=>(x.toUpperCase(),itemdesc.indexOf(x))).toMap
    if (targetdesc.indexOf("/") >= 0) { //如果存在斜杠分割的字符串则逐个寻找
    val targetlist = targetdesc.split("/").toList.map(x =>
      if(findEng(x) == ""){
        itemdesc.indexOf(x)
      }else{
        itemdesc_e.indexOf(x)   //英文则按全字匹配
        /*try {
           itemdesc_e_1.apply(x)
         } catch {
           case t: Exception => -1// TODO: handle error
         }*/
      })
      if (targetlist.min >= 0) { //如果每个分割的字符串都能找到则返回最前面的位置
        targetlist.min
      } else
        -1
    } else
    if(findEng(targetdesc) == ""){
      itemdesc.indexOf(targetdesc)
    }else{
      itemdesc_e.indexOf(targetdesc)
      /*try {
          itemdesc_e_1.apply(targetdesc)
        } catch {
          case t: Exception => -1// TODO: handle error
        }*/
    }
  }


  /**
    * findEng judge if the English is exactly only has English character
    * @param itemdesc target item description
    */

  private def findEng(itemdesc: String): String = {
    val pattern = "[a-zA-z]+".r
    val tmp = pattern.findAllIn(itemdesc).toList.mkString("")
    if (tmp.size == itemdesc.size) {
      tmp
    } else ""

  }

  /*
   * multifindDesc 按照分号查找,存在一个即可
   * @itemdesc target item description
   * @targetdesc multi desc
   */

  def multifindDesc(itemdesc: String, targetdesc: String): Int = {
    var flag = true
    val index = targetdesc.indexOf("/{")
    if(targetdesc.indexOf("/{")>=0){
      if(targetdesc.indexOf(";")>=0){
        val lst = targetdesc.split(";").filter(i=> !i.trim.isEmpty && i.indexOf("/{")>=0).map(x => findNagtiveDesc(itemdesc,x))
        if(lst.max>=0){
          flag = false
          return lst.max
        }
      }else {
        val tmp = findNagtiveDesc(itemdesc,targetdesc)
        if(tmp>=0){
          flag = false
          return tmp

        }
      }
    }

    if(flag){
      if (targetdesc.indexOf(";") >= 0) { //如果存在分号分割的字符串只要存在一个
      val targetlist = targetdesc.split(";").filter(x=> !x.trim.isEmpty && x.indexOf("/{")<0).map(x => findDesc(itemdesc, x)).toList // return the index of each string spilt by ";"
        if (targetlist.exists { _ > -1 }) {
          targetlist.filter(_ > -1).min
        } else
          -1
      } else
        findDesc(itemdesc, targetdesc)
    }else {
      -1
    }
  }

  def findNagtiveDesc(itemdesc: String, targetdesc: String):Int = {
    /*val targetlist = targetdesc.replace("/{", "|").split("\\|").toList.map(_.replace("}", "")).map(x=>findDesc1(itemdesc,x))
    if(targetlist.min>=0){
       targetlist.min
    }else {
      -1
    }*/
    val targetlist = targetdesc.replace("/{", "|").split("\\|").toList.map(_.replace("}", ""))
    var include = findDesc(itemdesc,targetlist(0))
    var notInclude = findDesc1(itemdesc,targetlist(1))
    if(include>=0 && notInclude <0){
      include
    }else {
      -1
    }
  }

  private def findDesc1(itemdesc: String, targetdesc: String): Int = {

    if (targetdesc.indexOf("$") >= 0) { //如果存在斜杠分割的字符串则逐个寻找
    val targetlist = targetdesc.split("\\u0024").toList.map(x => findDesc(itemdesc,x))
      if (targetlist.max>=0) { //如果每个分割的字符串有一个存在则返回当前的位置
        targetlist.max
      } else
        -1
    } else
      findDesc(itemdesc,targetdesc)
  }

  def PacksizeCoding(itemdesc: String, packname: String, packlist: List[Float]): List[Float] = {
    if (itemdesc.indexOf(packname) >= 0) {
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
        }
        else { rightPack = 1 }
        ((leftPack * rightPack) :: packlist) ++ result
      }
      else {
        (leftPack :: packlist) ++ result
      }
    }
    else List()
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
    var temp =  x.trim()
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
        try{(DSC2BSC(temp.head) :: y.reverse).reverse.mkString.toFloat}catch {case e:NumberFormatException=>1}
      } else if (!y.isEmpty) {
        num
      } else 1
    } else 1
  }

  //全角转半角
  private def DSC2BSC(input: Char): Char =
  {
    var i = input;
    if (i == '\u3000') {
      i = ' ';
    } else if (i > '\uFF00' && i < '\uFF5F') {
      i = (i - 65248).toChar;
    }
    return i;

  }


  def replaceC2E(CS: String): String = {
    if (CS == "克") {
      "G"
    } else if (CS == "千克") {
      "KG"
    } else if (CS == "毫升") {
      "ML"
    } else if (CS == "升") {
      "L"
    } else if (CS == "公升") {
      "L"
    } else if (CS == "公斤") {
      "KG"
    } else if (CS == "盎司") {
      "OZ"
    } else if (CS == "片") {
      "P"
    } else if (CS == "斤") {
      "J"
    } else CS

  }

  def packsizetransform(input:(String, String)):(String, String) = {
    var num = input._1.toFloat
    var unit = replaceC2E(input._2)
    if(unit == "KG"){
      num = 1000 * num
      unit = "G"
    }else if(unit == "L"){
      num = 1000 * num
      unit = "ML"
    }else if(unit == "OZ"){
      num = 28.3495231.toFloat * num
      unit = "G"
    }else if(unit == "J"){
      num = 500 * num
      unit = "G"
    }

    return (num.toString, unit)
  }


  def checkprice(input_price:String, pricerange:String):Boolean = {
    var lowprice = 0.toFloat
    var highprice = 0.toFloat
    var price = -1.toFloat
    try{
      lowprice = pricerange.split("-").head.toFloat
      highprice = pricerange.split("-").reverse.head.toFloat
      price = input_price.toFloat
    }catch {
      case e:NumberFormatException => println(e)
    }

    if(price > lowprice && price <= highprice){
      return true
    }else {
      return false
    }

  }

  def getBundleSegId(itemDesc:String,bundleSegConf:List[(String,String)]):String={
    val otherId = bundleSegConf.filter(_._2.toUpperCase().equals("OTHERS")).head._1
    val segConf = bundleSegConf.map{x=>(x._1,x._2.split("/"))}.filter(_._2.length>1)
    var segId = otherId
    breakable(
      segConf.map{x=>
        if(x._2.length == itemDesc.split("/").length){
          val indexLst = x._2.map { x => itemDesc.indexOf(x) }
          if(indexLst.min>=0){
            segId = x._1
            break
          }
        }
      }
    )
    return segId
  }

}

