package main.scala.com.nielsen.util

import java.net.URI
import java.text.SimpleDateFormat

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.util.control.Breaks._

/**
  * @author daiyu01
  */
class DataFormatUtil extends Serializable {
  def splitNew(source: String, splitTag: String, textQualifier: String): Array[String] = {
    var sourceArr = Array[String]()
    if (textQualifier == null) {
      sourceArr = source.split(splitTag, -1)
    } else {
      var newTag = splitTag.concat(textQualifier)
      sourceArr = source.split(newTag, -1).map(x => function(x, splitTag, textQualifier)).mkString("").substring(2).split(splitTag, -1).map(x => x.replace("*%$#", splitTag))
    }
    return sourceArr
  }


  def function(source: String, splitTag: String, textQualifier: String): String = {
    var tag = textQualifier.concat(splitTag)
    var index = source.indexOf(tag)
    if (index != -1) {
      splitTag.concat(textQualifier) + source.split(tag, -1).apply(0).replace(splitTag, "*%$#").concat(tag).concat(source.split(tag, -1).apply(1))
    } else {
      var index1 = source.indexOf(textQualifier)
      if (index1 != -1) {
        splitTag.concat(textQualifier) + source.replace(splitTag, "*%$#")
      } else {
        splitTag.concat(textQualifier) + source
      }
    }
  }

  def strTONum(str: String): Boolean = {
    var num = 0.0
    val pattern = "^(([0-9]+.[0-9]*[1-9][0-9]*)|([0-9]*[1-9][0-9]*.[0-9]+)|([0-9]*[1-9][0-9]*)|([0-9]+.[0-9]*)|([0-9]+))$".r
    if (str != null && str != "") {
      val numbers = pattern.findAllIn(str).toList.mkString("")
      if (numbers == str) {
        true
      } else {
        false
      }
    } else {
      false
    }

  }

  def strTODate(str: String): Boolean = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    try {
      val date = sdf.parse(str)
      true
    } catch {
      case t: Throwable => false // TODO: handle error
    }
  }

  def deleteExistPath(pathRaw: String) {
    val outPut = new Path(pathRaw)
    val hdfs = FileSystem.get(URI.create(pathRaw), new Configuration())
    if (hdfs.exists(outPut)) hdfs.delete(outPut, true)
  }

  def lengthAvilable(sourceArr: Array[String], columnnoArr: Array[String]): Boolean = {
    var flag: Boolean = false
    var len = columnnoArr.length
    breakable {
      for (i <- 0 to len - 1) {
        if (sourceArr.length == columnnoArr(i).toInt) {
          flag = true
          break
        }
      }
    }
    return flag
  }
}