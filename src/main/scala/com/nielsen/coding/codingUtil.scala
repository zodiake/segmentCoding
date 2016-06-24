package com.nielsen.coding

import scala.util.control.Breaks._
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import java.net.URI
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.shell.Delete

/**
 * @author daiyu01
 */
class codingUtil {
  def getSegId(lengthConf:List[(String,(String,String))],itemLength:Double):String={
    var segid=""
    var size = lengthConf.size
    breakable{
        for( i<- 0 to size-1){
          var min = lengthConf.apply(i)._2._1.toDouble
          var max : Double = 0.0
          if(lengthConf.apply(i)._2._2!="-"){
            max = lengthConf.apply(i)._2._2.toDouble
            if(itemLength >= min && itemLength < max) {
              segid = lengthConf.apply(i)._1
            } else  ""
          }else if(lengthConf.apply(i)._2._2.equals("-")){
            if(itemLength >= min) {
              segid = lengthConf.apply(i)._1
            }  else  ""
          }
          if(segid != "") break
        }
    }
     segid
  }
  
  def getFinalSegId(itemSeg:List[List[Float]],itemSegConf:List[(String, (String, String))],defaultSeg:String):String={
   var segid = defaultSeg 
   val size = itemSeg.size
    if(size>1){ //按照unitList抓取出来的数字不相同 
      segid 
    }else if(size == 1){
      val valueList = itemSeg.apply(0)
      if(valueList.size>1){ //相同单位抓取出来的数字不相同
        segid 
      }else{
        var itemWeight = valueList.apply(0).toDouble
        segid = getSegId(itemSegConf,itemWeight)
      }
    }
   return  segid
 }
  
  def itemmaster_segment_forFicalt(catcode: String, segment: String, itemfile: List[Array[String]]): List[List[(String, String)]] = {
    val itemlist = itemfile.filter(_(1) == catcode).filter(x => x(3).toUpperCase() == segment)
    val desc = itemlist.map(x => (x.head, x(5).toUpperCase()))
      .filter(x => x._2 != "").map(x=>(x._1,x._2.split("\\$").toList)).filter(_._2.size>=2).map(x=>(x._1,x._2(1))).filter(!_._2.isEmpty())

    val parentidlist = itemlist.map(x => (x.head, x.reverse.head))

    return List(desc, parentidlist)
  }
  
  def parseCMToMM(unit:String,list:List[Float]):List[Float] = {
    var cmList = list
    if(unit.equalsIgnoreCase("cm") && !list.isEmpty){
      cmList = list.map { x => x*10 }
    }
    cmList
  }
  
  
  def parseListToTuple(list:List[Float]):(String,String)={
    val size = list.size
    var left = ""
    var right = ""
    if(size == 2){
      left = list.apply(0).toString()
      right = list.apply(1).toString()
      if(left.toDouble > right.toDouble){
        var tmp = left
        left = right
        right = tmp
      }
    }else if(size == 1){
      left = list.apply(0).toString()
      right = "-"
    }
    (left,right)
  }
  
  def spLengthCoding(itemdesc: String, packname: String, packlist: List[Float]): List[Float] = {
    if (itemdesc.indexOf(packname) >= 0 || itemdesc.indexOf(packname.toUpperCase())>=0) {
      var packpos = itemdesc.indexOf(packname)
      if(packpos == -1) packpos = itemdesc.indexOf(packname.toUpperCase())
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
        (DSC2BSC(temp.head) :: y.reverse).reverse.mkString.toFloat
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
  def getSegNoForCombine(input:String):List[String]={
    var segNoCombineLst:List[String] = List()
    var splitChar = ""
    if(input.contains("/")){
      splitChar = "/"
    }else if(input.contains(";")){
      splitChar = ";"
    }
    segNoCombineLst = input.split(splitChar).map { x => x.split("-")(0) }.toList
    
    return segNoCombineLst
  }
  
  def getCombineSegId(segCombine:List[String],targetSegList:List[(String,String)]):String={
    var segId = ""
    breakable{
      for(targetSeg<-targetSegList){
        var resultLst = List[Boolean]()
        if(targetSeg._2.contains("/")){
          resultLst = targetSeg._2.split("/").toList.map(x => segCombine.contains(x))
          if(!resultLst.contains(false)) segId = targetSeg._1
        }
        if(targetSeg._2.contains(";")){
          resultLst = targetSeg._2.split(";").toList.map(x => segCombine.contains(x))
          if(!resultLst.contains(true)) segId = targetSeg._1
        }
        
        if(segId!="") break()
      }
    }
    return segId
  }
   def deleteExistPath(pathRaw:String){
    val outPut = new Path(pathRaw)
       val hdfs = FileSystem.get(URI.create(pathRaw),new Configuration())
       if(hdfs.exists(outPut)) hdfs.delete(outPut, true)
  }
}