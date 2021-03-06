package com.nielsen.dataformat

/**
  * @author daiyu01
  */
case class JDRaw(province: String,
                 city: String,
                 productId: String,
                 cateLv2: String,
                 cateLv3: String,
                 brand: String,
                 produtDesc: String,
                 salesPrice: String,
                 salesAmt: String,
                 totalAmt: String,
                 actBuyPrice: String,
                 date: String,
                 attribute: String,
                 global: String
                ) extends CsvFormat[scala.collection.Map[String, String]] {
  override def toCsvString(config: scala.collection.Map[String, String]): String = {
    val storeCode = (attribute, global) match {
      case ("京东平台", "非全球购") => "20127"
      case ("京东平台", "全球购") => "20125"
      case ("京东自营", "非全球购") => "20126"
      case ("京东自营", "全球购") => "20124"
    }
    //val oldFormat = "" + "," + "\"" + city + "\"" + "," + productId + "," + storeCode + "," + config.getOrElse(cateLv2, "") + "," + cateLv2 + "," + cateLv3 + "," + "" + "," + "" + "," + "\"" + brand + "\"" + "," + "\"" + "\"" + "," + "" + "," + "\"" + produtDesc + "\"" + "," + salesPrice + "," + 1 + "," + salesAmt + "," + totalAmt + "," + actBuyPrice + "," + date + "-01" + "," + "\"" + attribute + "\"" + "," + ""
    val newFormat =s""","${city}",${productId},${storeCode},${config.getOrElse(cateLv2, "")},${cateLv2},${cateLv3},,,"${brand.replaceAll(",", " ")}","",,"${produtDesc.replaceAll(",", " ")}",${salesPrice},1,${salesAmt},${totalAmt},${actBuyPrice},${date}-01,"${attribute.replaceAll(",", " ")}","""
    newFormat
  }
}