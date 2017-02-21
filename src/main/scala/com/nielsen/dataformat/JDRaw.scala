package com.nielsen.dataformat

/**
  * @author daiyu01
  */
case class JDRaw(val province: String,
                 val city: String,
                 val productId: String,
                 val cateLv2: String,
                 val cateLv3: String,
                 val brand: String,
                 val produtDesc: String,
                 val salesPrice: String,
                 val salesAmt: String,
                 val totalAmt: String,
                 val actBuyPrice: String,
                 val date: String,
                 val attribute: String,
                 val global: String
                ) extends CsvFormat[scala.collection.Map[String, String]] {
  override def toCsvString(config: scala.collection.Map[String, String]): String = {
    val storeCode = (attribute, global) match {
      case ("京东平台", "非全球购") => "20127"
      case ("京东平台", "全球购") => "20125"
      case ("京东自营", "非全球购") => "20126"
      case ("京东自营", "全球购") => "20124"
    }
    val oldFormat = "" + "," + "\"" + city + "\"" + "," + productId + "," + storeCode + "," + config.getOrElse(cateLv2, "") + "," + cateLv2 + "," + cateLv3 + "," + "" + "," + "" + "," + "\"" + brand + "\"" + "," + "\"" + "\"" + "," + "" + "," + "\"" + produtDesc + "\"" + "," + salesPrice + "," + 1 + "," + salesAmt + "," + totalAmt + "," + actBuyPrice + "," + date + "-01" + "," + "\"" + attribute + "\"" + "," + ""
    val newFormat =s""","${city}",${productId},${storeCode},${config.getOrElse(cateLv2, "")},${cateLv2},${cateLv3},,,"${brand.replaceAll(",", " ")}","",,"${produtDesc.replaceAll(",", " ")}",${salesPrice},1,${salesAmt},${totalAmt},${actBuyPrice},${date}-01,"${attribute.replaceAll(",", " ")}","""
    oldFormat + "\n" + newFormat
  }
}