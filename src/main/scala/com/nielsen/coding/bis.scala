/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.nielsen.coding

import java.net.URI

import com.nielsen.coding.function._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

object bis {

  def main(args: Array[String]) {
    val conf = new SparkConf()
    conf.setAppName("BisCoding")
    val sc = new SparkContext(conf)
    var catlist = List[String]()
    if (args(0) != "BIS") {
      System.err.println("This class can only be BIS!")
      System.exit(1)
    } else {
      catlist = List(args(0))
    }
    //val catlist = args(0).split(",")
    val raw_data_path = List("")
    //val des_data_path = List("aa_RESULT")

    for (path <- raw_data_path) {

      val templist = Array[String]()

      //val months = sc.textFile(args(2) + path, 336).map(_.split(",")).filter(_.size > 2).map(_(1).substring(6,8)).distinct.collect().toList

      //for (month <- months) {
      var ree = sc.parallelize(templist)
      for (catcode <- catlist) {

        val configFile = sc.textFile(args(1)).map(_.split(",")).collect().toList.filter(_ (1) == catcode)
        val testFile = sc.textFile(args(2) + path, 336).map(_.split(",")).filter(_ (0).toUpperCase() == catcode)
        //.filter(_(1).substring(6,8) == month)
        val seglist = configFile.filter(x => x(3) != "BRAND" && !x(3).contains("SUBBRAND") && x(3) != "PACKSIZE" && x(3) != "PRICETIER")
          .map(_ (3)).distinct
        val KRAFile = sc.textFile(args(3)).map(_.split(",")).collect().toList
        val subbrand_namelist = configFile.filter(_ (3).contains("SUBBRAND")).map(_ (3)).distinct
        val tempre = testFile.map(x => coding(x, configFile, seglist, subbrand_namelist, KRAFile))
        ree = tempre.map(_._2) ++ ree

      }
      deleteExistPath(args(4) + ".SEG")
      ree.filter(_ != "").distinct.saveAsTextFile(args(4) + ".SEG")
      //}


    }
    //brandresult_e.map(i => i._1 + "," + i._2).saveAsTextFile(args(4))
    sc.stop()
  }

  def deleteExistPath(pathRaw: String) {
    val outPut = new Path(pathRaw)
    val hdfs = FileSystem.get(URI.create(pathRaw), new Configuration())
    if (hdfs.exists(outPut)) hdfs.delete(outPut, true)
  }

  def itemmaster_brand(catcode: String, itemfile: List[Array[String]]): List[List[(String, String)]] = {
    val itemlist = itemfile.filter(_ (1) == catcode).filter(_ (3).toUpperCase() == "BRAND")
    val eccbrandlist = itemlist.map(x => (x.head, x(5).toUpperCase()))
      .filter(x => x._2 != "" && x._2 != "其他牌子")

    val eccshortdesc = itemlist.map(x => (x.head, x(6).toUpperCase()))
      .filter(x => x._2 != "")

    val eccmanulist = itemlist.map(x => (x.head, x(7).toUpperCase()))
      .filter(x => x._2 != "" && x._2 != "其他厂家")

    val ecbrandlist = itemlist.map(x => (x.head, x(8).toUpperCase()))
      .filter(x => x._2 != "" && x._2 != "O.Brand")

    val ecmanulist = itemlist.map(x => (x.head, x(9).toUpperCase()))
      .filter(x => x._2 != "" && x._2 != "O.Manu")

    val parentidlist = itemlist.map(x => (x.head, x.reverse.head))

    return List(eccbrandlist, eccshortdesc, eccmanulist, ecbrandlist, ecmanulist, parentidlist)
  }

  def itemmaster_subbrand(catcode: String, itemfile: List[Array[String]], subbrand_name: String): List[List[(String, String)]] = {
    val itemlist = itemfile.filter(_ (1) == catcode).filter(_ (3).toUpperCase() == subbrand_name)
    val eccbrandlist = itemlist.map(x => (x.head, x(5).toUpperCase()))
      .filter(x => x._2 != "" && x._2 != "其他牌子")

    val eccshortdesc = itemlist.map(x => (x.head, x(6).toUpperCase()))
      .filter(x => x._2 != "")

    val ecbrandlist = itemlist.map(x => (x.head, x(8).toUpperCase()))
      .filter(x => x._2 != "" && x._2 != "O.Brand")

    val parentidlist = itemlist.map(x => (x.head, x.reverse.head))

    return List(eccbrandlist, eccshortdesc, ecbrandlist, parentidlist)
  }

  def itemmaster_segment(catcode: String, segment: String, itemfile: List[Array[String]]): List[List[(String, String)]] = {
    val itemlist = itemfile.filter(_ (1) == catcode).filter(x => x(3).toUpperCase() == segment)
    val desc = itemlist.map(x => (x.head, x(5).toUpperCase()))
      .filter(x => x._2 != "")

    val parentidlist = itemlist.map(x => (x.head, x.reverse.head))

    return List(desc, parentidlist)
  }

  def itemmaster_packsize(catcode: String, itemfile: List[Array[String]]): List[String] = {
    val itemlist = itemfile.filter(_ (1) == catcode).filter(_ (3).toUpperCase() == "PACKSIZE")
    if (!itemlist.isEmpty) {
      val desc = itemlist.map(_ (5).toUpperCase())
        .filter(_ != "")
        .mkString(";")
        .split(";")
        .toList
      return desc
    } else return List()
  }

  def other_seg_coding(configFile: List[Array[String]], testFile: List[Array[String]],
                       segname: String, segno: String): List[(String, String)] = {
    val codingFuct = new codingFunc()
    val others = configFile.filter(_ (3) == segname).filter(_ (5) == "其他")
    var otherdesc = ""

    if (!others.isEmpty) {
      otherdesc = others.head.head
    } else {
      otherdesc = ""
    }

    val segmentresult = testFile.map(x => (
      codingFuct.MKWLC(
        x(4) + x(5).toUpperCase(),
        itemmaster_segment(x(0), segname, configFile)
      ), x(1)
      ))
      .map(x => if (x._1 == "") {
        (otherdesc, x._2)
      } else (x._1, x._2))
      .filter(_._1 != "")
      .map(x => (segno + "," + x._1, x._2))
    return segmentresult
  }


  def coding(item_raw: Array[String], configFile: List[Array[String]], seglist: List[String], subbrand_namelist: List[String], krafile: List[Array[String]]): (item, String) = {

    val codingFunc = new codingFunc()
    val item = new item()
    var item_result = List[String]()
    item.ITEMID = item_raw(1)

    item.perCode = item_raw(1).substring(0, 8)

    item.storeCode = item_raw(1).substring(8, 13)

    item.brand_description = (item_raw(2) + item_raw(3) + item_raw(4)).toUpperCase()

    item.description = (item_raw(2) + item_raw(3) + item_raw(4) + item_raw(5)).toUpperCase()

    var price = 0.toFloat
    try {
      price = item_raw(6).toFloat
    } catch {
      case e: NumberFormatException => println("item " + item_raw(1) + "'s price is not number.")
    }
    item.price = price.toString

    //cat result
    var cateId = ""
    if (!configFile.filter(_ (3) == "CATEGORY").filter(_ (1) == item_raw(0)).map(_ (0)).isEmpty) {
      cateId = configFile.filter(_ (3) == "CATEGORY").filter(_ (1) == item_raw(0)).map(_ (0)).apply(0)
    }
    item_result = item.ITEMID + "," + "20" + "," + cateId + "," + item_raw(0) + "," + item.perCode + "," + item.storeCode :: item_result

    val configFileNew = configFile.filter(_ (3) != "CATEGORY")

    //brand coding
    val brand_conf = itemmaster_brand(item_raw(0), configFileNew)
    val brand_id = codingFunc.MKWLC(item.brand_description, brand_conf)
    item.brand_id = brand_id

    if (brand_id != "") {
      val brandno = configFileNew.filter(_ (3) == "BRAND").map(_ (2)).distinct.head
      val segcode = configFileNew.filter(_ (0) == brand_id).map(_ (10)).head
      item_result = (item.ITEMID + "," + brandno + "," + item.brand_id + "," + segcode + "," + item.perCode + "," + item.storeCode) :: item_result
    } else {
      val brandno = configFileNew.filter(_ (3) == "BRAND").map(_ (2)).distinct.head
      item_result = (item.ITEMID + "," + brandno + "," + "TAOBAO_ZZZOTHER" + "," + "TAOBAO_ZZZOTHER" + "," + item.perCode + "," + item.storeCode) :: item_result
    }

    //packsize coding
    val packsize_conf = itemmaster_packsize(item_raw(0), configFileNew)
    val packsize = packsize_conf.map(y =>
      (if (!codingFunc.PacksizeCoding(item.description, y, List()).isEmpty) {
        codingFunc.PacksizeCoding(item.description, y, List()).max.toString() //两个相同单位取了最大的
      } else {
        ""
      }, y)).filter(_._1 != "").filter(_._1 != "0.0").map(x => codingFunc.packsizetransform(x))
      .distinct.sortBy(_._1).reverse.map(x => x._1 + x._2)
    if (!packsize.isEmpty) {
      item.packsize = packsize.head
    } else {
      item.packsize = ""
    }
    val packsizeno = configFileNew.filter(_ (3) == "PACKSIZE").map(_ (2)).distinct.head
    if (item.packsize != "") {
      val segcode = configFileNew.filter(_ (1) == item_raw(0)).filter(_ (3).toUpperCase() == "PACKSIZE").map(_ (10)).head
      item_result = (item.ITEMID + "," + packsizeno + "," + item.packsize + "," + item.packsize + "," + item.perCode + "," + item.storeCode) :: item_result
    } else {
      item_result = (item.ITEMID + "," + packsizeno + "," + "UNKNOWN" + "," + "UNKNOWN" + "," + item.perCode + "," + item.storeCode) :: item_result
    }

    //pricetier coding
    val pricetier_conf = configFileNew.filter(_ (3) == "PRICETIER")
    val price_result = pricetier_conf.filter(x => codingFunc.checkprice(item.price, x(5)))
    if (price_result.isEmpty || price_result.size > 1) {
      val pricetierno = configFileNew.filter(_ (3) == "PRICETIER").map(_ (2)).distinct.head
      item_result = (item.ITEMID + "," + pricetierno + "," + "UNKNOWN" + "," + "UNKNOWN" + "," + item.perCode + "," + item.storeCode) :: item_result
    } else {
      val pricetierno = price_result.head(2)
      item.pricetier = price_result.head(0)
      val segcode = configFileNew.filter(_ (0) == item.pricetier).map(_ (10)).head
      item_result = (item.ITEMID + "," + pricetierno + "," + item.pricetier + "," + segcode + "," + item.perCode + "," + item.storeCode) :: item_result
    }

    //subbrand coding
    for (subbrand_name <- subbrand_namelist) {
      val subbrand_conf = itemmaster_subbrand(item_raw(0), configFileNew.filter(_.reverse.head == item.brand_id), subbrand_name)
      val subbrand = codingFunc.MKWLC(item.description, subbrand_conf)
      item.subbrand_id = subbrand
      if (subbrand != "") {
        val subbrandno = configFileNew.filter(_ (3) == subbrand_name).map(_ (2)).distinct.head
        val segcode = configFileNew.filter(_ (0) == subbrand).map(_ (10)).head
        item_result = (item.ITEMID + "," + subbrandno + "," + item.subbrand_id + "," + segcode + "," + item.perCode + "," + item.storeCode) :: item_result
      } else if (!configFileNew.filter(_ (3) == subbrand_name).isEmpty) {
        val subbrandno = configFileNew.filter(_ (3) == subbrand_name).map(_ (2)).distinct.head
        item_result = (item.ITEMID + "," + subbrandno + "," + "UNKNOWN" + "," + "UNKNOWN" + "," + item.perCode + "," + item.storeCode) :: item_result
      }
    }

    for (seg <- seglist) {
      if (seg == "KRASEGMENT") {
        val kralist = krafile.map(x => (x(1).toInt, x(2))).toArray
        val bis = new Bis(item.brand_description, kralist)
        val kraitem = bis.kraseg
        val kraid = krafile.filter(x => x(2) == kraitem._2).head(0)
        val segno = configFileNew.filter(_ (3) == seg).map(_ (2)).distinct.head
        val segcode = configFileNew.filter(_ (0) == kraid).map(_ (10)).head
        item_result = (item.ITEMID + "," + segno + "," + kraid + "," + segcode + "," + item.perCode + "," + item.storeCode) :: item_result
      } else {
        val seg_conf = itemmaster_segment(item_raw(0), seg, configFileNew)
        val other_id = seg_conf.map(_.filter(_._2.toUpperCase() == "其他")).head
        val seg_id = codingFunc.MKWLC(item.description, seg_conf)
        if (seg_id != "") {
          item.updateDynamic(seg)(seg_id)
        } else if (!other_id.isEmpty) {
          item.updateDynamic(seg)(other_id.map(_._1).head)
        } else {
          item.updateDynamic(seg)("")
        }
        if (item.selectDynamic(seg) != "") {
          val segno = configFileNew.filter(_ (3) == seg).map(_ (2)).distinct.head
          val segcode = configFileNew.filter(_ (0) == item.selectDynamic(seg)).map(_ (10)).head
          item_result = (item.ITEMID + "," + segno + "," + item.selectDynamic(seg) + "," + segcode + "," + item.perCode + "," + item.storeCode) :: item_result
        }
      }
    }

    return (item, item_result.mkString("\n"))
  }

  def item2String(item: item): String = {
    return item.ITEMID + "," + "1526" + "," + item.packsize_new
  }

  def item_prepare(item_raw: item, item_des: item, seglist: List[String]): Boolean = {
    var check = true
    if (item_raw.brand_id == item_des.brand_id) {
      check = check && true
    } else {
      check = check && false
    }
    if (item_raw.subbrand_id != "") {
      if (item_raw.subbrand_id == item_des.subbrand_id) {
        check = check && true
      } else {
        check = check && false
      }
    }
    for (seg <- seglist) {
      if (item_raw.selectDynamic(seg) != "") {
        if (item_raw.selectDynamic(seg) == item_des.selectDynamic(seg)) {
          check = check && true
        } else {
          check = check && false
        }
      }
    }
    return check
  }

  class Bis(itemDesc: String, targetArr: Array[(Int, String)]) extends OrderTrait {
    def kraseg(): (Int, String) = {
      val other = targetArr.filter(_._2 == "其他")
      if (other.isEmpty) {
        System.err.println("Please add the segment of 'OTHER'!")
        System.exit(1)
      }
      val strresult = targetArr.map(x => (x._1, x._2, StrSearchB(itemDesc, x._2)))
        .filter(_._3 == true)
        .map(x => (x._1, x._2))
      if (strresult.isEmpty) {
        other.head
      } else {
        val primarr = PrimOrderAsc(itemDesc, strresult)
        if (primarr.size > 1) {
          PosiOrderDesc(itemDesc, strresult)
        } else if (primarr.size == 1) {
          primarr.head
        } else other.head
      }
    }
  }

}
