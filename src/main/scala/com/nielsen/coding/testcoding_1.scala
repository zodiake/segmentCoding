package com.nielsen.coding
import scala.io.Source
import java.io._
import java.nio.charset.StandardCharsets

object testcoding_1 {
  def main(args: Array[String]): Unit = {

    val codingFunc = new codingFunc()
    val catlist = List("IMF")
    //val configFile = Source.fromFile("test","utf-8").getLines().toList.map(_.split(",")).filter(_(1) == "SHAM")
    //val testFile = Source.fromFile("HC_UTF8","utf-8").getLines().toList.map(_.split(",")).filter(_(0) == "SHAM")
                    //.filter(_(1) == "20131405000000092")
    //val hieFile = Source.fromFile("hie.txt","utf-8").getLines().toList.map(_.split(","))
    //val seglist = configFile.filter(x => x(3) != "BRAND" && x(3) != "SUBBRAND" && x(3) != "PACKSIZE")
                  //.map(_(3)).distinct

    //var resultList = List.empty[item]
    var out = new java.io.FileWriter("output")

    for (catcode <- catlist) {

      val configFile = Source.fromFile("test","utf-8").getLines().toList.map(_.split(",")).filter(_(1) == "IMF")
      val testFile = Source.fromFile("HC_UTF8","utf-8").getLines().toList.map(_.split(",")).filter(_(0) == "IMF")
                      //.filter(_(1) == "20131405000000092")
      val seglist = configFile.filter(x => x(3) != "BRAND" && x(3) != "SUBBRAND" && x(3) != "PACKSIZE" && x(3) != "PRICETIER")
      .map(_(3)).distinct
      val subbrand_namelist = configFile.filter(_(3).contains("SUBBRAND")).map(_(3)).distinct
      val tempre = testFile.take(100).map(x => coding(x, configFile, seglist, subbrand_namelist))
      for(i <- tempre){
        println(i._2)
        out.write(i._2 + "\n")
      }
    }

    out.close()
    /*
    val brandresult = testFile.map(x => (
      codingFuct.MKWLC(
        (x(2) + "/" + x(3) + "/" + x(4)).toUpperCase(),
        itemmaster_brand(x(0), configFile)
      ), x
    ))
      .filter(_._1 != "")
      .map(x => ("2126," + x._1, x._2))

    var brandresult_e = brandresult.map(x => (x._1, x._2(1)))

    for (catcode <- catlist) {

      val brandcat = brandresult.filter(_._2.head == catcode)

      val segmenthie = hieFile.filter(x => x(2) != "CATEGORY" && x(2) != "MANUFACTURER"
        && x(2) != "BRAND").filter(_(0) == catcode)

      if (segmenthie.size != 0) {
        for (seg <- segmenthie) {

          if (seg(2) == "SUBBRAND") {
            val subgroup = configFile.filter(_(3) == "SUBBRAND")
              .filter(_(1) == catcode)
              .filter(_(10) != "-")
              //.map(x => (x(10),x))
              .groupBy(_(10))
            for (sub <- subgroup) {
              val subresult = brandcat.filter(_._1 == "2126," + sub._1)
                .map(_._2)
                .map(x => (codingFuct.MKWLC(
                  (x(2) + "/" + x(3) + "/" + x(4) + x(5)).toUpperCase(),
                  itemmaster_segment(x(0), seg(2), configFile)
                ), x(1)))
                .filter(_._1 != "")
                .map(x => (seg(1) + "," + x._1, x._2))
              brandresult_e = subresult ++ brandresult_e
            }
          } else if (seg(2) == "PACKSIZE") {
            val packname = itemmaster_packsize(catcode, configFile) //.foreach(println)

            val packsizelist = testFile.map(x => (packname.map(y =>
              (if (!codingFuct.PacksizeCoding((x(4) + x(5)).toUpperCase(), y, List()).isEmpty) {
                codingFuct.PacksizeCoding((x(4) + x(5)).toUpperCase(), y, List()).max.toString() //两个相同单位取了最大的
              } else { "" }, y)).filter(_._1 != "").filter(_._1 != "0.0")
              .map(x => x._1 + x._2).mkString("/"), x(1)))
              .filter(_._1 != "")
              .map(x => (seg(1) + "," + x._1, x._2))

            brandresult_e = packsizelist ++ brandresult_e

          } else {
            val others = configFile.filter(_(3) == seg(2)).filter(_(5) == "其他")
            var otherdesc = ""

            if (!others.isEmpty) {
              otherdesc = others.head.head
            } else {
              otherdesc = ""
            }

            val segmentresult = testFile.map(x => (
              codingFuct.MKWLC(
                x(4) + x(5).toUpperCase(),
                itemmaster_segment(x(0), seg(2), configFile)
              ), x(1)
            ))
              .map(x => if (x._1 == "") { (otherdesc, x._2) } else (x._1, x._2))
              .filter(_._1 != "")
              .map(x => (seg(1) + "," + x._1, x._2))

            brandresult_e = segmentresult ++ brandresult_e

          }
        }
      }
    }

    val catresult = testFile.map(x => ("20," + x(0), x(1)))
    brandresult_e = catresult ++ brandresult_e

    println("-----------")
    var out = new java.io.FileWriter("output")

    for (i <- brandresult_e)
      out.write(i._1 + "," + i._2 + "\n")
    out.close()
    */
  }


  def itemmaster_brand(catcode: String, itemfile: List[Array[String]]): List[List[(String, String)]] = {
    val itemlist = itemfile.filter(_(1) == catcode).filter(_(3).toUpperCase() == "BRAND")
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

  def itemmaster_subbrand(catcode: String, itemfile: List[Array[String]], subbrand_name:String): List[List[(String, String)]] = {
    val itemlist = itemfile.filter(_(1) == catcode).filter(_(3).toUpperCase() == subbrand_name)
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
    val itemlist = itemfile.filter(_(1) == catcode).filter(x => x(3).toUpperCase() == segment)
    val desc = itemlist.map(x => (x.head, x(5).toUpperCase()))
      .filter(x => x._2 != "")

    val parentidlist = itemlist.map(x => (x.head, x.reverse.head))

    return List(desc, parentidlist)
  }

  def itemmaster_packsize(catcode: String, itemfile: List[Array[String]]): List[String] = {
    val itemlist = itemfile.filter(_(1) == catcode).filter(_(3).toUpperCase() == "PACKSIZE")
    if (!itemlist.isEmpty) {
      val desc = itemlist.map(_(5).toUpperCase())
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
    val others = configFile.filter(_(3) == segname).filter(_(5) == "其他")
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
      .map(x => if (x._1 == "") { (otherdesc, x._2) } else (x._1, x._2))
      .filter(_._1 != "")
      .map(x => (segno + "," + x._1, x._2))
    return segmentresult
  }


  def coding(item_raw:Array[String], configFile:List[Array[String]], seglist:List[String], subbrand_namelist:List[String]):(item,String) = {
    
    val codingFunc = new codingFunc()
    val item = new item()
    var item_result = List[String]()
    item.ITEMID = item_raw(1)

    item.brand_description = (item_raw(2) + item_raw(3) + item_raw(4)).toUpperCase()

    item.description = (item_raw(2) + item_raw(3) + item_raw(4) + item_raw(5)).toUpperCase()

    var price = 0.toFloat
    try{
      price = item_raw(6).toFloat
    }catch {
      case e:NumberFormatException => println("item " + item_raw(1) + "'s price is not number.")
    }
    item.price = price.toString

    //cat result
    item_result = item.ITEMID + "," + "20" + "," + item_raw(0) :: item_result

    
    //brand coding
    val brand_conf = itemmaster_brand(item_raw(0),configFile)
    val brand_id = codingFunc.MKWLC(item.brand_description, brand_conf)
    item.brand_id = brand_id

    if(brand_id != ""){
      val brandno = configFile.filter(_(3) == "BRAND").map(_(2)).distinct.head
      item_result = (item.ITEMID + "," + brandno + "," + item.brand_id) :: item_result
    }else {
      val brandno = configFile.filter(_(3) == "BRAND").map(_(2)).distinct.head
      item_result = (item.ITEMID + "," + brandno + "," + "TAOBAO_ZZZOTHER") :: item_result
    }

    //packsize coding
    val packsize_conf = itemmaster_packsize(item_raw(0), configFile)
    val packsize = packsize_conf.map(y =>
      (if (!codingFunc.PacksizeCoding(item.description, y, List()).isEmpty) {
        codingFunc.PacksizeCoding(item.description, y, List()).max.toString() //两个相同单位取了最大的
      } else { "" }, y)).filter(_._1 != "").filter(_._1 != "0.0").map(x => codingFunc.packsizetransform(x))
      .distinct.sortBy(_._1).reverse.map(x => x._1 + x._2)
    if(!packsize.isEmpty){
      item.packsize = packsize.head
    }else {
      item.packsize = ""
    }
    if(item.packsize != ""){
      val packsizeno = configFile.filter(_(3) == "PACKSIZE").map(_(2)).distinct.head
      item_result = (item.ITEMID + "," + packsizeno + "," + item.packsize) :: item_result
    }

    //pricetier coding
    val pricetier_conf = configFile.filter(_(3) == "PRICETIER")
    val price_result = pricetier_conf.filter(x => codingFunc.checkprice(item.price, x(5)))
    if(price_result.isEmpty || price_result.size > 1 || item_raw(0) == "DIAP"){

    }else {
      val pricetierno = price_result.head(2)
      item.pricetier = price_result.head(0)
      item_result = (item.ITEMID + "," + pricetierno + "," + item.pricetier) :: item_result
    }

    //subbrand coding
    for(subbrand_name <- subbrand_namelist){
      val subbrand_conf = itemmaster_subbrand(item_raw(0), configFile.filter(_.reverse.head == item.brand_id), subbrand_name)
      val subbrand = codingFunc.MKWLC(item.description, subbrand_conf)
      item.subbrand_id = subbrand
      if(subbrand != ""){
        val subbrandno = configFile.filter(_(3) == subbrand_name).map(_(2)).distinct.head
        item_result = (item.ITEMID + "," + subbrandno + "," + item.subbrand_id) :: item_result
      }else if(!configFile.filter(_(3) == subbrand_name).isEmpty){
        val subbrandno = configFile.filter(_(3) == subbrand_name).map(_(2)).distinct.head
        item_result = (item.ITEMID + "," + subbrandno + "," + "UNKNOWN") :: item_result
      }
    }

    for(seg <- seglist) {
      if(item_raw(0) == "FB" && seg == "CAPACITY"){
        val capacity_conf = configFile.filter(_(3) == "CAPACITY")
        val capacity_result = capacity_conf.filter(x => codingFunc.checkprice(item.packsize.replace("ML",""), x(5)))
        if(capacity_result.isEmpty || capacity_result.size > 1 ){

        }else {
          val capacityno = capacity_result.head(2)
          item.capacity = capacity_result.head(0)
          item_result = (item.ITEMID + "," + capacityno + "," + item.capacity) :: item_result
        }

      } else {
        val seg_conf = itemmaster_segment(item_raw(0), seg, configFile)
        val other_id = seg_conf.map(_.filter(_._2.toUpperCase() == "其他")).head
        val seg_id = codingFunc.MKWLC(item.description, seg_conf)
        println(seg_id)
        if(seg_id != ""){
          item.updateDynamic(seg)(seg_id)
        }else if(!other_id.isEmpty){
          item.updateDynamic(seg)(other_id.map(_._1).head)
        }else {
          item.updateDynamic(seg)("")
        }
        if(item.selectDynamic(seg) != ""){
          val segno = configFile.filter(_(3) == seg).map(_(2)).distinct.head
          item_result = (item.ITEMID + "," + segno + "," + item.selectDynamic(seg)) :: item_result
        }
      }
    }

    return (item,item_result.mkString("\n"))
  }

  def item2String(item: item):String = {
    return item.ITEMID + "," + "1526" + "," + item.packsize_new
  }

  def item_prepare(item_raw: item, item_des: item, seglist:List[String]):Boolean ={
    var check = true
    if(item_raw.brand_id == item_des.brand_id){
      check = check && true
    }else {
      check = check && false
    }
    if(item_raw.subbrand_id != ""){
      if(item_raw.subbrand_id == item_des.subbrand_id){
        check = check && true
      }else {
        check = check && false
      }
    }
    for(seg <- seglist){
      if(item_raw.selectDynamic(seg) != ""){
        if(item_raw.selectDynamic(seg) == item_des.selectDynamic(seg)){
          check = check && true
        }else {
          check = check && false
        }
      }
    }
    return check
  }

}
