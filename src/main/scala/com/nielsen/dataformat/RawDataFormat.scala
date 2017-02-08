package com.nielsen.dataformat

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

//商品编码,商品名称,销售数量,付款金额,系统国际条码,自报国际条码
case class RedBaby(code: String, name: String, amount: String, price: String)

object RawDataFormat {

  def main(args: Array[String]) {

    var pathRaw = ""
    var pathUni = ""
    var year = ""
    var dataSrc = ""
    var pathFilter = ""
    if (args.length == 4) {
      pathRaw = args(0) //RAW DATA位置
      pathUni = args(1) //TELECOM代码表的位置
      year = args(2)
      dataSrc = args(3) //TELECOM//苏宁易购
    } else if (args.length == 3) {
      if (args(2) == "RedBaby") {
        pathRaw = args(0)
        dataSrc = "RedBaby"
        year = args(1)
      } else if (args(2) == "JDNEW") {
        pathRaw = args(0)
        dataSrc = "JDNew"
        pathUni = args(1) // website code map
      } else {
        pathRaw = args(0)
        pathUni = args(1)
        dataSrc = args(2) //1mall/京东商城/飞牛网
      }
    } else if (args.length == 5) {
      pathRaw = args(0) //raw data path
      pathUni = args(1) // website code map
      pathFilter = args(2) // filter data path 
      year = args(3)
      dataSrc = args(4) //QBT
    } else if (args.length == 2) {
      pathRaw = args(0) //raw data path
      dataSrc = args(1) //TMTB
    }

    val conf = new SparkConf()
    //conf.setAppName("DataFormat")
    //conf.setMaster("local")
    val sc = new SparkContext(conf)
    val templist = Array[String]()
    var universe = sc.parallelize(templist)
    if (pathUni != "") {
      universe = sc.textFile(pathUni)
    }
    val uniCommRDD = universe.map(row => {
      val colValue = row.split(",")
      new etailerUniverse(
        colValue(1), //storeCode
        colValue(0) //storeWeb 
      )
    })
    var storeCodeArr = uniCommRDD.map(x => (x.storeWeb, x.storeCode)).filter(_._1.equalsIgnoreCase(replaceE2C(dataSrc))).map(_._2).collect()
    var storeCode = ""
    if (!storeCodeArr.isEmpty) {
      storeCode = storeCodeArr.apply(0)
    }

    /*
    "" + "," + "\"" + x.location + "\"" + "," + x.prodId + "," + storeCode + "," + "1mall" + "," + x.cateLvl2Nm + "," + x.cateLvl3Nm + ","
    + x.cateLvl4Nm + "," + x.cateLvLeaf + "," + "\"" + x.brand.replace(",", " ") + "\"" + "," + "\"" + "\"" + "," +
      "" + "," + "\"" + x.prodNm.replace(",", " ") + "\"" + "," + x.soldPrice + ","
    + 1 + "," + x.soldNum + "," + x.soldAmount + "," + x.soldActAmount + "," + x.date + "-01" + "," + "\"" + x.bizType.replace(",", " ") + "\"" + "," + ""
    */

    if (dataSrc == "RedBaby") {
      val data = sc.textFile(pathRaw).map(_.split("\t")).filter(i => i.length >= 4).map {
        i =>
          val r = RedBaby(i(0), i(1), i(2), i(3))
          val unit = if (r.amount.toDouble == 0) 0 else r.price.toDouble / r.amount.toDouble
          s""","",${r.code},52491,REDBABY,,,,,"","",,"${r.name}",${unit},1,${r.amount},${r.price},${unit},${year.substring(0, 4)}-${year.substring(6)}-01,"","""
      }
      deleteExistPath(pathRaw)
      data.saveAsTextFile(pathRaw.concat(".REFORMAT"))
      //data.take(1).foreach(println)
    }

    if (dataSrc == "TELECOMCROSS") {
      val teleRaw = sc.textFile(pathRaw).filter(x => x.split("\t").length == 9)
      val rawRDD = teleRaw.map(row => {
        val colValue = row.split("\t")
        new TelecomRaw(
          colValue(0).replace("^\\xEF\\xBB\\xBF", ""), //date
          colValue(1), //prodDesc
          colValue(2), //cateLv2
          colValue(3), //cateLv1
          colValue(4), //storeID  to match teleCode
          colValue(5).replace("\\", "").replace("N", "0"), //pv
          colValue(6).replace("\\", "").replace("N", "0"), //uv
          colValue(7), //website
          colValue(8) //location
        )
      })

      val uniRDD = universe.map(row => {
        val colValue = row.split(",")
        new etailerUniverse(
          colValue(1), //storeCode
          colValue(2) //teleCode
        )
      })

      val rawRDD2 = rawRDD.map(x => (x.storeID, (year, x.prodDesc, x.cateLv2, x.cateLv1, x.storeID, x.pv, x.uv, x.website, x.location)))
      val uniRDD2 = uniRDD.map(x => (x.storeWeb, x.storeCode))
      val teleReformatRaw = rawRDD2.leftOuterJoin(uniRDD2).filter(!_._2._2.isEmpty)
      val result = teleReformatRaw.map(x => "" + "," + "\"" + x._2._1._9 + "\"" + "," + "" + "," + x._2._2.get + "," + "TELECOMCROSS" + "," + x._2._1._3.replace(",", "") + "," + x._2._1._4 + "," + "" + "," + "" + "," + "\"" + "\"" + "," + "\"" + "\"" + "," + "" + "," + "\"" + x._2._1._2.replace(",", "") + "\"" + "," + x._2._1._6 + "," + "1" + "," + x._2._1._7 + "," + x._2._1._6 + "," + x._2._1._6 + "," + year.substring(0, 4) + "-" + year.substring(6) + "-" + "01" + "," + "\"" + x._2._1._8 + "\"" + "," + "")
      deleteExistPath(pathRaw)
      result.saveAsTextFile(pathRaw.concat(".REFORMAT"))
    }

    if (dataSrc == "TELECOM") {
      val teleRaw = sc.textFile(pathRaw).filter(x => x.split("\t").length == 9)
      val rawRDD = teleRaw.map(row => {
        val colValue = row.split("\t")
        new TelecomRaw(
          colValue(0).replace("^\\xEF\\xBB\\xBF", ""), //date
          colValue(1), //prodDesc
          colValue(2), //cateLv2
          colValue(3), //cateLv1
          colValue(4), //storeID  to match teleCode
          colValue(5).replace("\\", "").replace("N", "0"), //pv
          colValue(6).replace("\\", "").replace("N", "0"), //uv
          colValue(7), //website
          colValue(8) //location  
        )
      })

      val uniRDD = universe.map(row => {
        val colValue = row.split(",")
        new etailerUniverse(
          colValue(1), //storeCode
          colValue(2) //teleCode 
        )
      })

      val rawRDD2 = rawRDD.map(x => (x.storeID, (year, x.prodDesc, x.cateLv2, x.cateLv1, x.storeID, x.pv, x.uv, x.website, x.location)))
      val uniRDD2 = uniRDD.map(x => (x.storeWeb, x.storeCode))
      val teleReformatRaw = rawRDD2.leftOuterJoin(uniRDD2).filter(!_._2._2.isEmpty)
      val result = teleReformatRaw.map(x => "" + "," + "\"" + x._2._1._9 + "\"" + "," + "" + "," + x._2._2.get + "," + "TELECOM" + "," + x._2._1._3.replace(",", "") + "," + x._2._1._4 + "," + "" + "," + "" + "," + "\"" + "\"" + "," + "\"" + "\"" + "," + "" + "," + "\"" + x._2._1._2.replace(",", "") + "\"" + "," + x._2._1._6 + "," + "1" + "," + x._2._1._7 + "," + x._2._1._6 + "," + x._2._1._6 + "," + year.substring(0, 4) + "-" + year.substring(6) + "-" + "01" + "," + "\"" + x._2._1._8 + "\"" + "," + "")
      deleteExistPath(pathRaw)
      result.saveAsTextFile(pathRaw.concat(".REFORMAT"))
    }
    if (dataSrc == "1MALL") {
      val oneMallRaw = sc.textFile(pathRaw).filter(x => x.split(",").length == 15)
        .filter(x => {
          val col = x.split(",")(14)
          col == "商城" || col == "商城海购"
        })
      val rawRdd = oneMallRaw.map(raw => {
        val value = raw.split(",")
        new OneMallRaw(
          value(5), //prodId
          value(10),
          value(1), //cateLvl2Nm
          value(2), //cateLvl3Nm
          value(3), //cateLvl4Nm
          value(11),
          value(6), //brand
          value(7), //prodNm
          value(13), //销售价格
          value(12), //销售数量
          value(13), //总金额
          value(13), //实际买价
          value(14), //bizType
          value(0).replace("^\\xEF\\xBB\\xBF", "") //mm
        )
      })

      val result = rawRdd.map(x => {
        if (x.bizType == "商城")
          storeCode = "21008"
        else if (x.bizType == "商城海购")
          storeCode = "90278"
        "" + "," + "\"" + x.location + "\"" + "," + x.prodId + "," + storeCode + "," + "1mall" + "," + x.cateLvl2Nm + "," + x.cateLvl3Nm + "," + x.cateLvl4Nm + "," + x.cateLvLeaf + "," + "\"" + x.brand.replace(",", " ") + "\"" + "," + "\"" + "\"" + "," + "" + "," + "\"" + x.prodNm.replace(",", " ") + "\"" + "," + x.soldPrice + "," + 1 + "," + x.soldNum + "," + x.soldAmount + "," + x.soldActAmount + "," + x.date + "-01" + "," + "\"" + x.bizType.replace(",", " ") + "\"" + "," + ""
      })
      deleteExistPath(pathRaw)
      result.saveAsTextFile(pathRaw.concat(".REFORMAT"))
    }
    if (dataSrc == "SUNING") {
      val suNingRaw = sc.textFile(pathRaw)
        .map(i => {
          if (i.endsWith("\t"))
            i + " "
          else
            i
        })
        .filter(x => x.split("\t").length == 12)
      val rawRdd = suNingRaw.map(row => {
        val value = row.split("\t")
        new SuNingRaw(
          value(1), //storeid
          value(0).replace("^\\xEF\\xBB\\xBF", ""), //city
          value(2), //itemCode
          value(9), //prodCat
          value(10).trim(), //EAN
          value(3), //desc
          value(6).toDouble + value(8).toDouble, //salesvalue
          value(4).toDouble + value(7).toDouble, //salesvolume
          value(5).toDouble + value(8).toDouble,
          value(5).toDouble //payvalue
        )
      })

      val result = rawRdd.map(x => {
        if (x.pay != 0.toDouble)
          storeCode = "20270"
        else
          storeCode = "20271"
        x.storeId + "," + "\"" + x.city + "\"" + "," + x.itemCode + "," + storeCode + "," + "SUNING" + "," + x.prodCate.replace(",", "") + "," + x.EAN + "," + "" + "," + "" + "," + "\"" + "\"" + "," + "\"" + "\"" + "," + "" + "," + "\"" + x.desc.replace(",", "") + "\"" + "," + x.salesValue * 10000 + "," + 1 + "," + x.salesVolumn + "," + x.payValue * 10000 + "," + x.payValue * 10000 + "," + year.substring(0, 4) + "-" + year.substring(6) + "-" + "01" + "," + "\"" + " " + "\"" + "," + ""
      })
      deleteExistPath(pathRaw)
      result.saveAsTextFile(pathRaw.concat(".REFORMAT"))
    }

    if (dataSrc == "QBTBD") {
      val qbtRaw = sc.textFile(pathRaw).filter(x => x.split(",").length == 10).map(x => x.replace("\"", ""))
      val rawRDD = qbtRaw.map(raw => {
        val value = raw.split(",")
        new QBTRaw(
          value(6), //website
          value(5), //brand
          value(7), //prod_desc
          value(0).replace("^\\xEF\\xBB\\xBF", "") + "||" + value(1) + "||" + value(2) + "||" + value(3) + "||" + value(4), // attribute
          value(9),
          value(8))
      })

      val uniRDD = universe.map(row => {
        val colValue = row.split(",")
        new etailerUniverse(
          colValue(1), //storeCode
          colValue(3) //teleCode 
        )
      })

      val filterRaw = sc.textFile(pathFilter).collect().toList
      val afterFilterRaw = rawRDD.filter { x => !filterRaw.contains(x.website) }.map(x => (x.website, (x.website, x.brand, x.prodDesc, x.attribute, x.temp, x.url)))
      val websiteCodeRdd = uniRDD.map(x => (x.storeWeb, x.storeCode))
      val formatRaw = afterFilterRaw.leftOuterJoin(websiteCodeRdd).filter(!_._2._2.isEmpty)
      val result = formatRaw.map(x => "" + "," + "\"" + "\"" + "," + "" + "," + x._2._2.get + "," + "QBT" + "," + "" + "," + "" + "," + "" + "," + x._2._1._6 + "," + "\"" + x._2._1._2 + "\"" + "," + "\"" + "\"" + "," + x._2._1._5 + "," + "\"" + x._2._1._3 + "\"" + "," + 0 + "," + 0 + "," + 0 + "," + 0 + "," + 0 + "," + year.substring(0, 4) + "-" + year.substring(6) + "-" + "01" + "," + "\"" + x._2._1._4 + "\"" + "," + "")
      deleteExistPath(pathRaw)
      result.saveAsTextFile(pathRaw.concat(".REFORMAT"))
    }
    if (dataSrc == "JD") {
      val jdRaw = sc.textFile(pathRaw).filter(x => x.split("\t").length == 11)
      val rawRdd = jdRaw.map(raw => {
        val value = raw.split("\t")
        new JDRaw(
          value(6),
          value(7),
          value(0).replace("^\\xEF\\xBB\\xBF", ""),
          value(2),
          value(3),
          value(4),
          value(1).replace(",", " "),
          divide(value(10), value(9)),
          value(9),
          value(10),
          divide(value(10), value(9)),
          value(5),
          value(8),"")
      })
      /*
        18,"1586",10090124297,20120,JD,休闲食品,糖果/巧克力,,,"福派园","",,"福派园 花生咸牛轧糖500g 手工牛扎糖 休闲零食品喜糖果软糖奶糖 花生味500g",16.2,1,1,16.2,16.2,2016-06-01,"京东平台",
       */
      val result = rawRdd.map(x => {
        if (x.attribute == "京东平台")
          storeCode = "20121"
        else if (x.attribute == "京东自营")
          storeCode = "20120"
        x.province + "," + "\"" + x.city + "\"" + "," + x.productId + "," + storeCode + "," + "JD" + "," + x.cateLv2 + "," + x.cateLv3 + "," + "" + "," + "" + "," + "\"" + x.brand + "\"" + "," + "\"" + "\"" + "," + "" + "," + "\"" + x.produtDesc + "\"" + "," + x.salesPrice + "," + 1 + "," + x.salesAmt + "," + x.totalAmt + "," + x.actBuyPrice + "," + x.date + "-01" + "," + "\"" + x.attribute + "\"" + "," + ""
      })
      deleteExistPath(pathRaw)
      result.saveAsTextFile(pathRaw.concat(".REFORMAT"))
    }

    if (dataSrc == "JDNew") {
      //新的ｊｄ数据多了一列年月，忽略即可
      val jdRaw = sc.textFile(pathRaw).filter(x => x.split("\t").length == 14)
      val rawRdd = jdRaw.map(raw => {
        val value = raw.split("\t")
        new JDRaw(
          value(6),
          value(7),
          value(0).replace("^\\xEF\\xBB\\xBF", ""),
          value(2),
          value(3),
          value(4).replace(","," "),
          value(1).replace(",", " "),
          divide(value(10), value(9)),
          value(9),
          value(10),
          divide(value(10), value(9)),
          value(5),
          value(8),
          value(12))
      })
      /*
        18,"1586",10090124297,20120,JD,休闲食品,糖果/巧克力,,,"福派园","",,"福派园 花生咸牛轧糖500g 手工牛扎糖 休闲零食品喜糖果软糖奶糖 花生味500g",16.2,1,1,16.2,16.2,2016-06-01,"京东平台",
       */
      val categoryLevel2Map = sc.broadcast(sc.textFile("hdfs://10.250.33.107:9000/CONF_DATA/JDMODEL_CONFIG.TXT").map(_.split(",")).map(i => (i(0), i(1))).collectAsMap()).value

      val result = rawRdd.map(x => {
        storeCode = (x.attribute, x.global) match {
          case ("京东平台", "非全球购") => "20127"
          case ("京东平台", "全球购") => "20125"
          case ("京东自营", "非全球购") => "20126"
          case ("京东自营", "全球购") => "20124"
        }
        "" + "," + "\"" + x.city + "\"" + "," + x.productId + "," + storeCode + "," + categoryLevel2Map.getOrElse(x.cateLv2, "") + "," + x.cateLv2 + "," + x.cateLv3 + "," + "" + "," + "" + "," + "\"" + x.brand + "\"" + "," + "\"" + "\"" + "," + "" + "," + "\"" + x.produtDesc + "\"" + "," + x.salesPrice + "," + 1 + "," + x.salesAmt + "," + x.totalAmt + "," + x.actBuyPrice + "," + x.date + "-01" + "," + "\"" + x.attribute + "\"" + "," + ""
      })
      deleteExistPath(pathRaw)
      result.saveAsTextFile(pathRaw.concat(".REFORMAT"))
    }

    if (dataSrc == "FEINIU") {
      val feiNiuRaw = sc.textFile(pathRaw).filter(x => x.split("\t").length == 12)
      val rawRdd = feiNiuRaw.map(raw => {
        val value = raw.split("\t")
        new FeiNiuRaw(
          value(1),
          value(7),
          value(2),
          value(3),
          value(4),
          value(6),
          value(8).replace(",", " "),
          divide(value(10), value(9)),
          value(9),
          value(10),
          divide(value(10), value(9)),
          value(5),
          formatDate(value(0)),
          value(11))
      })
      val result = rawRdd.map(x => {
        if (x.source == "自营")
          storeCode = "20510"
        else if (x.source == "商城")
          storeCode = "20511"
        else if (x.source == "环球购")
          storeCode = "20512"
        "" + "," + "\"" + x.city + "\"" + "," + x.prodId + "," + storeCode + "," + "FEINIU" + "," + x.cateLv2 + "," + x.cateLv3 + "," + x.cateLv4 + "," + "" + "," + "\"" + x.brand + "\"" + "," + "\"" + "" + "\"" + "," + "" + "," + "\"" + x.prodDesc + "\"" + "," + x.salesPrice + "," + "1" + "," + x.salesAmount + "," + x.totalAmt.trim() + "," + x.actBuyPrice + "," + x.date + "," + "\"" + x.source + "\"" + "," + x.attribute.trim()
      })
      deleteExistPath(pathRaw)
      result.saveAsTextFile(pathRaw.concat(".REFORMAT"))
    }
    //.filter { x => x(9).length()>9 && x(9).head.isDigit }
    if (dataSrc == "TMTB") {
      val tmtbRaw = sc.textFile(pathRaw).map { x => x.split(",") }
        .filter { x => x.length == 13 }.filter { x => x(8) != "0" && x(11) != "0" }.map { raw =>
        val shopId = raw(0)
        val prodId = raw(1)
        val catLv1 = raw(4)
        val catLv2 = raw(5)
        val catLv3 = raw(6)
        val brand = raw(7)
        val prodDesc = raw(2)
        val price = raw(11)
        val unit = raw(8)
        val totalAmount = parseStrTONum(unit) * parseStrTONum(price)
        val realPrice = price
        val transDate = raw(9)
        val attr = raw(12)
        (shopId, prodId, catLv1, catLv2, catLv3, brand, prodDesc, price, unit, totalAmount, realPrice, transDate, attr)
      }
      val result = tmtbRaw.map(x => x._1 + "," + "\"" + "\"" + "," + x._2 + "," + "C" + "," + x._3 + "," + x._4 + "," + x._5 + "," + "" + "," + "" + "," + "\"" + x._6 + "\"" + "," + "\"" + "\"" + "," + "" + "," + "\"" + x._7 + "\"" + "," + x._8 + "," + "" + "," + x._9 + "," + x._10 + "," + x._11 + "," + x._12 + "," + "\"" + x._13 + "\"" + "," + "")
      deleteExistPath(pathRaw)
      result.saveAsTextFile(pathRaw.concat(".REFORMAT"))
    }

  }

  def parseStrTONum(str: String): Double = {
    var num = 0.0
    val pattern = "^(([0-9]+.[0-9]*[1-9][0-9]*)|([0-9]*[1-9][0-9]*.[0-9]+)|([0-9]*[1-9][0-9]*))$".r
    if (str != null && str != "") {
      val numbers = pattern.findAllIn(str).toList.mkString("")
      if (numbers == str) {
        num = numbers.toDouble
      }
    }
    num
  }

  def divide(str1: String, str2: String): String = {
    if (str2.toDouble != 0) {
      (str1.toDouble / str2.toDouble).toString()
    } else {
      (str1.toDouble / 1).toString()
    }
  }

  def replaceE2C(eng: String): String = {
    if (eng == "JD") {
      "京东商城"
    } else if (eng == "SUNING") {
      "苏宁易购"
    } else if (eng == "FEINIU") {
      "飞牛网"
    } else eng
  }

  def deleteExistPath(pathRaw: String) {
    val outPut = new Path(pathRaw.concat(".REFORMAT"))
    val hdfs = FileSystem.get(URI.create(pathRaw.concat(".REFORMAT")), new Configuration())
    if (hdfs.exists(outPut)) hdfs.delete(outPut, true)
  }

  def formatDate(str: String): String = {
    val n = str.indexOf("年")
    val m = str.indexOf("月")
    var year = ""
    var month = ""
    var date = ""
    if (n > 0 && m > 0) {
      year = str.substring(0, n)
      month = str.substring(n + 1, m)
      if (month.length() == 1) month = "0" + month
      date = year + "-" + month + "-" + "01"
    }
    return date
  }

  // TBD
  def formatNum(num: Double, decimal: Int): String = {
    var result = ""

    return result

  }

  def formatDate1(str: String): String = {
    var date = ""
    if (!str.isEmpty() && str.length() > 9 && str.head.isDigit) {
      if (str == "0") {

      } else {
        val arr = str.split("-")
        var year = arr(0)
        var month = arr(1)
        var day = arr(2)
        if (month.length() == 1) {
          month = "0".concat(month)
        }
        if (day.length() == 1) {
          day = "0".concat(day)
        }
        date = year + "-" + month + "-" + day
      }
    }
    return date
  }

}