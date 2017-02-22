package com.nielsen.dataformat

/**
  * @author daiyu01
  */
case class OneMallRaw(prodId: String,
                      location: String,
                      cateLvl2Nm: String,
                      cateLvl3Nm: String,
                      cateLvl4Nm: String,
                      cateLvLeaf: String,
                      brand: String,
                      prodNm: String,
                      soldPrice: String,
                      soldNum: String,
                      soldAmount: String,
                      soldActAmount: String,
                      bizType: String,
                      date: String
                     ) extends CsvFormat[Unit] {
  override def toCsvString(year: Unit): String = {
    val storeCode = if (bizType == "商城") "21008" else if (bizType == "商城海购") "90278"
    val unitPrice = if (soldNum.toDouble == 0) 0 else soldAmount.toDouble / soldNum.toDouble
    /*
    val oldFormat = "" + "," + "\"" + location + "\"" + "," + prodId + "," + storeCode + "," + "1mall" + "," + cateLvl2Nm + "," + cateLvl3Nm + "," + cateLvl4Nm + "," + cateLvLeaf + "," + "\"" +
      brand.replace(",", " ") + "\"" +
      "," + "\"" + "\"" + "," + "" + "," + "\"" + prodNm.replace(",", " ") + "\"" + "," + soldPrice + "," + 1 + "," + soldNum + "," + soldAmount + "," + soldActAmount + "," + date + "-01" + "," + "\"" + bizType.replace(",", " ") + "\"" + "," + ""
      */
    val newFormat =
      s""","${location}",${prodId},${storeCode},1mall,${cateLvl2Nm},${cateLvl3Nm},${cateLvl4Nm},${cateLvLeaf},"${brand.replaceAll(",", "")}","",,"${prodNm.replaceAll(",", " ")}",${unitPrice},1,${soldNum},${soldAmount},${unitPrice},${date}-01,"${bizType.replaceAll(",", " ")}","""
    newFormat
  }
}
  
