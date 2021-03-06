package com.nielsen.coding

import java.net.URI

import com.nielsen.coding.bis.Bis
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.control.Breaks.{break, breakable}

/*
 *@param args
 * IMF
 * hdfs://hkgrherfpp016:9000/CONF_DATA/SEGCONF
 * /home/zodiake/Downloads/test_file/ONE_LINE
 * hdfs://hkgrherfpp016:9000/CONF_DATA/CATCONF
 * hdfs://hkgrherfpp016:9000/TEST/Raphael/RAPHAEL_ALL_JAR_TEST/TMTB_20171401.SEGCO
 * PACKSIZE
 * hdfs://hkgrherfpp016:9000/CONF_DATA/krasegment
 * tmtb
 */
object HistoryCoding {

  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: <inputFile> <outputFile> <brand_list>")
      System.exit(1)
    }

    val conf = new SparkConf()
    //conf.setAppName("TotalCoding")
    //conf.setMaster("local[*]")
    val sc = new SparkContext(conf)
    var catlist = List[String]()
    if (args(0) == "ALL") {
      catlist = sc.textFile(args(1)).map(_.split(",")).collect().toList.map(_ (1)).distinct
    } else {
      catlist = args(0).split(",").toList
    }

    //add for bis
    var KRAFile = List[Array[String]]()
    //add end
    var cateCodeCombine = ""
    var segNoCombine = ""
    val rddResult = for (catcode <- catlist) yield {
      if (catcode == "BIS") {
        KRAFile = sc.textFile(args(6)).map(_.split(",")).collect().toList
      }
      //new requirement for combine two segment to one segment for the specific catecode
      if (catcode == "SKIN") {
        cateCodeCombine = "SKIN"
        segNoCombine = "WHITEUV,ANTIPORE"
      }
      if (catcode == "FACIA") {
        cateCodeCombine = "FACIA"
        segNoCombine = "ANTIAPORE,MOISLIGHT,OILACNEP"
      }
      val configFile = sc.textFile(args(1)).map(_.split(",")).collect().toList.filter(_ (1) == catcode)
      val cateConf = sc.textFile(args(3)).map(_.split(",")).collect().toList //add for match bundedpack
      val testFile = sc.textFile(args(2), 336).map(x => transCateCode(x, cateConf).split(",")).filter(_ (0).toUpperCase() == catcode)
      val seglist = configFile.filter(x => x(3) != "BRAND" && !x(3).contains("SUBBRAND") && x(3) != "PACKSIZE" && x(3) != "PRICETIER" && x(3) != "CATEGORY")
        .map(_ (3)).distinct
      val subbrand_namelist = configFile.filter(_ (3).contains("SUBBRAND")).map(_ (3)).distinct
      val tempre = testFile.map(x => coding(x, configFile, seglist, subbrand_namelist, args(5), KRAFile, cateCodeCombine, segNoCombine, args(7)))

      tempre.map(_._2.mkString("\n"))
    }
    //deleteExistPath(args(4) + "_" + i + ".SEG")
    deleteExistPath(args(4) + ".SEG")
    rddResult.reduce((rdd1, rdd2) => rdd1.union(rdd2)).distinct().saveAsTextFile(args(4) + ".SEG")
    //ree.filter(_ != "").distinct.saveAsTextFile(args(4) + "_" + i + ".SEG")
    //rddResult.reduce((rdd1, rdd2) => rdd1.union(rdd2)).take(10).foreach(println)

    sc.stop()
  }

  def coding(item_raw: Array[String], configFile: List[Array[String]], seglist: List[String], subbrand_namelist: List[String], codingType: String, krafile: List[Array[String]], cateCodeCombine: String, segCombine: String, t: String): (item, List[String]) = {

    var brandFlg = false
    var packsizeFlg = false
    var pricetierFlg = false
    var subbrandFlg = false
    var segmentFlg = false

    if (codingType.equalsIgnoreCase("ALL")) {
      brandFlg = true
      packsizeFlg = true
      pricetierFlg = true
      subbrandFlg = true
      segmentFlg = true
    } else {
      val codingTypeArr = codingType.split(",")
      codingTypeArr.foreach { x =>
        x match {
          case "BRAND" => brandFlg = true
          case "PACKSIZE" => packsizeFlg = true
          case "PRICETIER" => pricetierFlg = true
          case "SUBBRAND" => subbrandFlg = true
          case "SEGMENT" => segmentFlg = true
        }
      }
    }

    val codingFunc = new codingFunc()
    val item = new item()
    val codingUtil = new codingUtil()
    var item_result = List[String]()
    item.ITEMID = item_raw(1)

    item.perCode = item_raw(1).substring(0, 8)

    item.storeCode = item_raw(1).substring(8, 13)

    item.brand_description = (item_raw(2) + " " + item_raw(3) + " " + item_raw(4)).toUpperCase()

    item.description = (item_raw(2) + " " + item_raw(3) + " " + item_raw(4) + " " + item_raw(5)).toUpperCase()

    item.bundleSeg = item_raw(8)
    item.catCode = item_raw(0)

    var catCode = item_raw(0)

    var price = 0.toFloat
    try {
      price = item_raw(6).toFloat
    } catch {
      case e: NumberFormatException => println("item " + item_raw(1) + "'s price is not number.")
    }
    item.price = price.toString

    //cat result
    var cateId = ""
    if (!configFile.filter(_ (3) == "CATEGORY").filter(_ (1) == catCode).map(_ (0)).isEmpty) {
      cateId = configFile.filter(_ (3) == "CATEGORY").filter(_ (1) == catCode).map(_ (0)).apply(0)
    }
    item_result = item.ITEMID + "," + "20" + "," + cateId + "," + catCode + "," + item.perCode + "," + item.storeCode :: item_result

    val configFileNew = configFile.filter(x => x(3) != "CATEGORY")
    //brand coding
    val brand_conf = itemmaster_brand(catCode, configFileNew) //List[List[(String, String)]] --eccbrandlist, eccshortdesc, eccmanulist, ecbrandlist, ecmanulist, parentidlist
    if (brandFlg) {
      val brand_id = codingFunc.MKWLC(item.brand_description, brand_conf)
      item.brand_id = brand_id
      if (brand_id != "") {
        val brandno = configFileNew.filter(_ (3) == "BRAND").map(_ (2)).distinct.head // brand no
        val segcode = configFileNew.filter(_ (0) == brand_id).map(_ (10)).head
        item_result = (item.ITEMID + "," + brandno + "," + item.brand_id + "," + segcode + "," + item.perCode + "," + item.storeCode) :: item_result
      } else {
        var brandno = ""
        if (!configFileNew.filter(_ (3) == "BRAND").map(_ (2)).distinct.isEmpty) {
          brandno = configFileNew.filter(_ (3) == "BRAND").map(_ (2)).distinct.head
        }
        item_result = (item.ITEMID + "," + brandno + "," + "TAOBAO_ZZZOTHER" + "," + "TAOBAO_ZZZOTHER" + "," + item.perCode + "," + item.storeCode) :: item_result
      }
    }

    //packsize coding

    val packsize_conf = itemmaster_packsize(catCode, configFileNew)
    if (packsizeFlg) {
      if (t == "TMTB") {
        def packsize(str: String): String = {
          val pack = packsize_conf.map { y =>
            val r = codingFunc.PacksizeCoding(str, y, List())
            val p = if (!r.isEmpty) {
              r.max.toString() //两个相同单位取了最大的
            } else {
              ""
            }
            (p, y)
          }.filter(i => i._1 != "" && i._1 != "0.0").map(x => codingFunc.packsizetransform(x))
            .distinct.sortBy(_._1.toDouble).reverse.map(x => x._1 + x._2)
          if (!pack.isEmpty) {
            pack.head //所有单位之间区最大的
          } else {
            ""
          }
        }

        def getFinalPacksize(pack: String): String = {
          if (pack.isEmpty) {
            "UNKNOWN"
          } else {
            pack
          }
        }

        item.packsize1 = ""
        item.packsize2 = ""
        if (!configFileNew.filter(_ (3) == "PACKSIZE").map(_ (2)).isEmpty) {
          val brandDesc = item_raw(2) + " " + item_raw(3) + " " + item_raw(4)
          val attr = item_raw(5)
          val packsize1 = packsize(brandDesc.toUpperCase)
          val packsize2 = packsize(attr.toUpperCase)

          val packsizeno = configFileNew.filter(_ (3) == "PACKSIZE").map(_ (2)).distinct.head

          val p1 = getFinalPacksize(packsize1)
          val p2 = getFinalPacksize(packsize2)
          item.packsize1 = packsize1
          item.packsize2 = packsize2
          item_result = (item.ITEMID + "," + packsizeno + "," + p1 + "," + p2 + "," + item.perCode + "," + item.storeCode) :: item_result
        }
      } else {
        val packsize = packsize_conf.map(y => {
          val list = codingFunc.PacksizeCoding(item.description, y, List())
          (if (!list.isEmpty) {
            list.max.toString() //两个相同单位取了最大的
          } else {
            ""
          }, y)
        }).filter(_._1 != "").filter(_._1 != "0.0").map(x => codingFunc.packsizetransform(x))
          .distinct.sortBy(_._1.toDouble).reverse.map(x => x._1 + x._2)
        if (!packsize.isEmpty) {
          item.packsize = packsize.head //所有单位之间区最大的
        } else {
          item.packsize = ""
        }
        if (item.packsize != "") {
          val packsizeno = configFileNew.filter(_ (3) == "PACKSIZE").map(_ (2)).distinct.head
          val segcode = configFileNew.filter(_ (1) == catCode).filter(_ (3).toUpperCase() == "PACKSIZE").map(_ (10)).head
          item_result = (item.ITEMID + "," + packsizeno + "," + item.packsize + "," + item.packsize + "," + item.perCode + "," + item.storeCode) :: item_result
        } else {
          var packsizeno = ""
          if (!configFileNew.filter(_ (3) == "PACKSIZE").map(_ (2)).isEmpty) {
            packsizeno = configFileNew.filter(_ (3) == "PACKSIZE").map(_ (2)).distinct.head
            item_result = (item.ITEMID + "," + packsizeno + "," + "UNKNOWN" + "," + "UNKNOWN" + "," + item.perCode + "," + item.storeCode) :: item_result
          }
        }
      }
    }

    //pricetier coding
    /* if(pricetierFlg){
       val pricetier_conf = configFileNew.filter(_(3) == "PRICETIER")
       val price_result = pricetier_conf.filter(x => codingFunc.checkprice(item.price, x(5)))
       if(price_result.isEmpty || price_result.size > 1 || catCode == "DIAP"){

       }else {
         val pricetierno = price_result.head(2)
         item.pricetier = price_result.head(0)
         val segcode = configFileNew.filter(_(0) == item.pricetier).map(_(10)).head
         item_result = (item.ITEMID + "," + pricetierno + "," + item.pricetier + "," + segcode + "," + item.perCode + "," + item.storeCode) :: item_result
       }
     }
     */
    //subbrand coding
    if (subbrandFlg) {
      for (subbrand_name <- subbrand_namelist) {
        val subbrand_conf = itemmaster_subbrand(catCode, configFileNew.filter(_.reverse.head == item.brand_id), subbrand_name)
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
    }

    var filterSegLst: List[String] = List()
    //segment coding
    if (segmentFlg) {
      //for new requirement  combine two segment to one segment
      var finalSeglst: List[String] = List()
      if (catCode == cateCodeCombine) {
        if (segCombine.contains(",")) filterSegLst = segCombine.split(",").toList else filterSegLst = List(segCombine)
        finalSeglst = seglist.filter { x => !filterSegLst.contains(x) }
      } else {
        finalSeglst = seglist
      }
      //bunded seg coding
      for (seg <- finalSeglst) {
        if (catCode == "FB" && seg == "CAPACITY") {
          if (t == "TMTB") {
            val capacity_conf = configFileNew.filter(x => x(3) == "CAPACITY" && x(1) == "FB")
            val capacityno = capacity_conf.head(2)
            val p1 = try {
              item.packsize1.replace("ML", "").toFloat
            } catch {
              case e: NumberFormatException => 0
            }
            val p2 = try {
              item.packsize2.replace("ML", "").toFloat
            } catch {
              case e: NumberFormatException => 0
            }
            val p = Math.max(p1, p2)
            val capacity_result = capacity_conf.filter(x => codingFunc.checkprice(p.toString, x(5)))
            if (capacity_result.isEmpty || capacity_result.size > 1) {
              item_result = (item.ITEMID + "," + capacityno + "," + "UNKNOWN" + "," + "UNKNOWN" + "," + item.perCode + "," + item.storeCode) :: item_result
            } else {
              val capacityno = capacity_result.head(2)
              item.capacity = capacity_result.head(0)
              val segcode = configFileNew.filter(_ (0) == item.capacity).map(_ (10)).head
              item_result = (item.ITEMID + "," + capacityno + "," + item.capacity + "," + segcode + "," + item.perCode + "," + item.storeCode) :: item_result
            }
          } else {
            val capacity_conf = configFileNew.filter(x => x(3) == "CAPACITY" && x(1) == "FB")
            val capacityno = capacity_conf.head(2)
            val capacity_result = capacity_conf.filter(x => codingFunc.checkprice(item.packsize.replace("ML", ""), x(5)))
            if (capacity_result.isEmpty || capacity_result.size > 1) {
              item_result = (item.ITEMID + "," + capacityno + "," + "UNKNOWN" + "," + "UNKNOWN" + "," + item.perCode + "," + item.storeCode) :: item_result
            } else {
              val capacityno = capacity_result.head(2)
              item.capacity = capacity_result.head(0)
              val segcode = configFileNew.filter(_ (0) == item.capacity).map(_ (10)).head
              item_result = (item.ITEMID + "," + capacityno + "," + item.capacity + "," + segcode + "," + item.perCode + "," + item.storeCode) :: item_result
            }
          }

        } else if ((catCode.contains("_BUNDLE") || catCode.contains("BANDEDPACK_")) && seg == "SUBCATEGORY") {
          val bundleSegConf = itemmaster_bundleSeg(catCode, configFileNew)
          val bundleSegId = codingFunc.getBundleSegId(item.bundleSeg, bundleSegConf)
          item.bundleSegId = bundleSegId
          val bundleSegNo = configFileNew.filter(_ (3) == "SUBCATEGORY").map(_ (2)).distinct.head
          val bundlecode = configFileNew.filter(_ (0) == bundleSegId).map(_ (10)).head
          if (bundleSegId != "") {
            item_result = (item.ITEMID + "," + bundleSegNo + "," + item.bundleSegId + "," + bundlecode + "," + item.perCode + "," + item.storeCode) :: item_result
          } else {
            //  item_result = (item.ITEMID + "," + bundleSegNo + "," + "UNKNOWN" + "," + bundlecode + "," + item.perCode + "," + item.storeCode) :: item_result
          }
        } else {
          var seg_id = ""
          var other_id = List[(String, String)]()
          if (catCode == "IMF" && seg == "STAGE") {
            val seg_conf = itemmaster_segment_forFirst(catCode, seg, configFileNew)
            other_id = seg_conf.map(_.filter(_._2.toUpperCase() == "其他")).head
            seg_id = codingFunc.MKWLC(item.description, seg_conf)
            if (seg_id == "") {
              val seg_conf = itemmaster_segment_forSecond(catCode, seg, configFileNew)
              other_id = seg_conf.map(_.filter(_._2.toUpperCase() == "其他")).head ::: other_id
              seg_id = codingFunc.MKWLC(item.description, seg_conf)
            }
            if (seg_id == "") {
              val seg_conf = itemmaster_segment_forThird(catCode, seg, configFileNew)
              other_id = seg_conf.map(_.filter(_._2.toUpperCase() == "其他")).head ::: other_id
              seg_id = codingFunc.MKWLC(item.description, seg_conf)
            }
            if (seg_id == "") {
              seg_id = "650006"
            }
            //Remap the “Pre段” to “1段” and ”6段” to  “3段” in Aptamil & Nutrilon
            if ((item.brand_id == "630069" || item.brand_id == "630853" || item.brand_id == "630075") && seg == "STAGE") {
              if (seg_id == "650004" && (item.description.contains("6段") || item.description.contains("六段"))) {
                seg_id = "650003"
              }
            }
          }
          //add this part for V2.1 requirement change
          else if (catCode == "SP" && seg == "LENGTH") {
            val seg_conf = itemmaster_segment("SP", "LENGTH", configFileNew)
            val length_conf = seg_conf.head.filter(_._2.contains("MM")).map(x => (x._1, codingUtil.spLengthCoding(x._2, "MM", List()))).map(x => (x._1, codingUtil.parseListToTuple(x._2)))
            val unitList = List("mm", "cm")
            val itemLengthforMM = unitList.map { x => codingUtil.parseCMToMM(x, codingUtil.spLengthCoding(item.description, x, List())).distinct }.map(x => x.filter { x => x > 50 }).filter { x => !x.isEmpty }.distinct
            seg_id = codingUtil.getFinalSegId(itemLengthforMM, length_conf, "5160005")
            if (seg_id == "") {
              other_id = seg_conf.map(_.filter(_._2.toUpperCase() == "其他")).head
              seg_id = codingFunc.MKWLC(item.description, seg_conf.map(x => x.filter(!_._2.contains("MM"))))
            }
          } //add end
          // add BIS logic here
          else if (catCode == "BIS" && seg == "KRASEGMENT") {
            val kralist = krafile.map(x => (x(1).toInt, x(2))).toArray
            val bis = new Bis(item.brand_description, kralist)
            val kraitem = bis.kraseg
            seg_id = krafile.filter(x => x(2) == kraitem._2).head(0)
          } //add end
          else {
            val seg_conf = itemmaster_segment(catCode, seg, configFileNew)
            other_id = seg_conf.map(_.filter(x => x._2.toUpperCase() == "其他" || x._2.toUpperCase() == "其它其它")).head
            seg_id = codingFunc.MKWLC(item.description, seg_conf)
          }
          if (seg_id != "") {
            item.updateDynamic(seg)(seg_id)
          } else if (!other_id.isEmpty) {
            item.updateDynamic(seg)(other_id.map(_._1).head)
          } else {
            item.updateDynamic(seg)("UNKNOWN")
          }
          if (item.selectDynamic(seg) != "") {
            val segno = configFileNew.filter(_ (3) == seg).map(_ (2)).distinct.head
            var segcodeLst = configFileNew.filter(_ (0) == item.selectDynamic(seg)).map(_ (10))
            var segcode = "UNKNOWN"
            if (!segcodeLst.isEmpty) {
              segcode = segcodeLst.head
            }
            item.updateDynamic(segno)(segcode)
            item_result = (item.ITEMID + "," + segno + "," + item.selectDynamic(seg) + "," + segcode + "," + item.perCode + "," + item.storeCode) :: item_result
          }
        }
      }

      //add for new requirment   segCombine -- segtype
      if (catCode == cateCodeCombine && !filterSegLst.isEmpty) {
        for (segTypeFilter <- filterSegLst) {
          val combineSegConf = itemmaster_segment(catCode, segTypeFilter, configFileNew).head
          val relatedSegLst = codingUtil.getSegNoForCombine(combineSegConf.head._2) //segNo list
          var combineSegDesc = relatedSegLst.map(segNo => segNo + "-" + item.selectDynamic(segNo))
          var segid = codingUtil.getCombineSegId(combineSegDesc, combineSegConf)
          // combineSegConf.map(segDesc=>(codingFunc.multifindDesc(combineSegDesc, segDesc._2),segDesc._1)).filter(_._1>=0).map(_._2).head
          val segno = configFileNew.filter(_ (3) == segTypeFilter).map(_ (2)).distinct.head
          var segcode = "UNKNOWN"
          if (segid == "") {
            segid = "UNKNOWN"
          } else {
            segcode = configFileNew.filter(_ (0) == segid).map(_ (10)).head
          }
          item.updateDynamic(segCombine)(segid)
          item_result = (item.ITEMID + "," + segno + "," + item.selectDynamic(segCombine) + "," + segcode + "," + item.perCode + "," + item.storeCode) :: item_result
        }
      }
      //add end
    }

    return (item, item_result)
  }

  //itemfile -- > category information file
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

  //for prepare un-known stage keywords -- first priority
  def itemmaster_segment_forFirst(catcode: String, segment: String, itemfile: List[Array[String]]): List[List[(String, String)]] = {
    val itemlist = itemfile.filter(_ (1) == catcode).filter(x => x(3).toUpperCase() == segment).filter(x => x(4) == "不知道")
    val desc = itemlist.map(x => (x.head, x(5).toUpperCase()))
      .filter(x => x._2 != "").filter(_._2 != "")

    val parentidlist = itemlist.map(x => (x.head, x.reverse.head))

    return List(desc, parentidlist)
  }

  //for prepare contains "岁，月" keywords -- second priority
  def itemmaster_segment_forSecond(catcode: String, segment: String, itemfile: List[Array[String]]): List[List[(String, String)]] = {
    val itemlist = itemfile.filter(_ (1) == catcode).filter(x => x(3).toUpperCase() == segment).filter(x => x(4) != "不知道")
    val desc = itemlist.map(x => (x.head, x(5).toUpperCase()))
      .filter(x => x._2 != "").map(x => (x._1, (x._2.split(";").toList.filter(_.contains("岁")) ::: x._2.split(";").toList.filter(_.contains("月"))).mkString(";"))).filter(_._2 != "")

    val parentidlist = itemlist.map(x => (x.head, x.reverse.head))

    return List(desc, parentidlist)
  }

  //for prepare doesn't contains "岁，月" keywords -- third priority
  def itemmaster_segment_forThird(catcode: String, segment: String, itemfile: List[Array[String]]): List[List[(String, String)]] = {
    val itemlist = itemfile.filter(_ (1) == catcode).filter(x => x(3).toUpperCase() == segment).filter(x => x(4) != "不知道")
    val desc = itemlist.map(x => (x.head, x(5).toUpperCase()))
      .filter(x => x._2 != "").map(x => (x._1, x._2.split(";").toList.filter(!_.contains("岁")).filter(!_.contains("月")).mkString(";"))).filter(_._2 != "")
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

  def itemmaster_bundleSeg(catcode: String, itemfile: List[Array[String]]): List[(String, String)] = {
    val itemlist = itemfile.filter(_ (1) == catcode).filter(_ (3).toUpperCase() == "SUBCATEGORY")
    val bundleSeglst = itemlist.map(x => (x.head, x(5).toUpperCase()))
      .filter(x => x._2 != "")

    return bundleSeglst
  }

  def itemmaster_segment(catcode: String, segment: String, itemfile: List[Array[String]]): List[List[(String, String)]] = {
    val itemlist = itemfile.filter(_ (1) == catcode).filter(x => x(3).toUpperCase() == segment)
    val desc = itemlist.map(x => (x.head, x(5).toUpperCase()))
      .filter(x => x._2 != "")

    val parentidlist = itemlist.map(x => (x.head, x.reverse.head))

    return List(desc, parentidlist)
  }

  //add for match bundedpack
  def transCateCode(item: String, cateConf: List[Array[String]]): String = {
    val cateTrans = cateConf.filter(x => x(0).equalsIgnoreCase(item.split(",")(0)))
    if (!cateTrans.isEmpty) {
      item.replace(item.split(",")(0), cateTrans.map(_ (1)).apply(0)) + "," + item.split(",")(0)
    } else {
      item + "," + " "
    }
  }

  def deleteExistPath(pathRaw: String) {
    val outPut = new Path(pathRaw)
    val hdfs = FileSystem.get(URI.create(pathRaw), new Configuration())
    if (hdfs.exists(outPut)) hdfs.delete(outPut, true)
  }

  def itemTBRmove(item: String, itemIdLst: List[String]): Boolean = {
    var flag = true //代表不需要被移除
    val itemArr = item.split(",")
    breakable(
      itemIdLst.map { x =>
        if (x.equalsIgnoreCase(itemArr(0)) && itemArr(1) == "1526") {
          flag = false
          break
        }
      }
    )

    return flag
  }

  def other_seg_coding(configFile: List[Array[String]], testFile: List[Array[String]],
                       segname: String, segno: String): List[(String, String)] = {
    val codingFuct = new codingFunc
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
}

