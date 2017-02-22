package com.nielsen.dataformat

/**
  * @author daiyu01
  */
case class FeiNiuRaw(city: String,
                     prodId: String,
                     cateLv2: String,
                     cateLv3: String,
                     cateLv4: String,
                     brand: String,
                     prodDesc: String,
                     salesPrice: String,
                     salesAmount: String,
                     totalAmt: String,
                     actBuyPrice: String,
                     attribute: String,
                     date: String,
                     source: String
                    ) extends CsvFormat[Unit] {
  override def toCsvString(config: Unit): String = {
    val storeCode = if (source == "自营") "20510" else if (source == "商城") "20511" else if (source == "环球购") "20512"
    val oldFormat = "" + "," + "\"" + city + "\"" + "," + prodId + "," + storeCode + "," + "FEINIU" + "," + cateLv2 + "," + cateLv3 + "," + cateLv4 + "," + "" + "," + "\"" + brand + "\"" + "," + "\"" + "" + "\"" + "," + "" + "," + "\"" + prodDesc + "\"" + "," + salesPrice + "," + "1" + "," + salesAmount + "," + totalAmt.trim() + "," + actBuyPrice + "," + date + "," + "\"" + source + "\"" + "," + attribute.trim()
    val newFormat =
      s""","${city}",${prodId},${storeCode},FEINIU,${cateLv2},${cateLv3},${cateLv4},,"${brand.replaceAll(",", " ")}","",,"${prodDesc.replaceAll(",", " ")}",${salesPrice},1,${salesAmount},${totalAmt.trim},${salesPrice},${date},"${source.replaceAll(",", " ")}",${attribute.trim}"""
    newFormat
  }
}