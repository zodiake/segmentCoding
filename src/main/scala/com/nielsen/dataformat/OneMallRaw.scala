package com.nielsen.dataformat

/**
  * @author daiyu01
  */
case class OneMallRaw(val prodId: String,
                      val location: String,
                      val cateLvl2Nm: String,
                      val cateLvl3Nm: String,
                      val cateLvl4Nm: String,
                      val cateLvLeaf: String,
                      val brand: String,
                      val prodNm: String,
                      val soldPrice: String,
                      val soldNum: String,
                      val soldAmount: String,
                      val soldActAmount: String,
                      val bizType: String,
                      val date: String
                     ) extends CsvFormat[Unit] {
  override def toCsvString(year: Unit): String = {
    val storeCode = if (bizType == "商城") "21008" else if (bizType == "商城海购") "90278"
    val unitPrice = if (soldNum.toDouble == 0) 0 else soldAmount.toDouble / soldNum.toDouble
    val newFormat =
      s""","${location}",${prodId},${storeCode},1mall,${cateLvl2Nm},${cateLvl3Nm},${cateLvl4Nm},${cateLvLeaf},"${brand.replaceAll(",", "")}","",,"${prodNm.replaceAll(",", " ")}",${unitPrice},1,${soldNum},${soldAmount},${unitPrice},${date}-01,"${bizType.replaceAll(",", " ")}","""
    newFormat
  }
}
  
