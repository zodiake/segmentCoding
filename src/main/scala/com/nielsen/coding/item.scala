package com.nielsen.coding
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import java.nio.charset.StandardCharsets
import org.apache.spark.rdd._
import scala.io.Source
import scala.language.dynamics

class item extends java.io.Serializable with Dynamic {

  var map = Map.empty[String, String]

  def selectDynamic(name: String) =
    map get name getOrElse sys.error("method not found")

  def updateDynamic(name: String)(value: String) {
    map += name -> value
  }

  //def applyDynamicNamed(name: String)(args: (String, Any)*) =

}
