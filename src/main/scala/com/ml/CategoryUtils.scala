package com.ml

/**
  * Created by wangqi08 on 1/7/2016.
  */
object CategoryUtils {
  def categoryToInt(s: String): Int = {
    lazy val list = List("SKIN", "NC", "IMF", "SKIN_BUNDLE", "DCONF", "NUTS", "CMSNACK", "HAIDS", "PHS", "BBC", "CF", "TONER", "FACIA", "HAIR_BUNDLE", "RICE", "SHAM", "BIS", "FACIALMASK", "AIR", "TB", "BANDEDPACK_OTHERS", "RAZOR", "DWL", "HOUS", "BABYW", "HGLOVES", "LAUND", "BATHROOMT", "SP", "DCRP", "PWASH", "VITAMINS", "CONF", "MILK", "FACIALT", "LIPPALM", "SNACK", "EYEMASK", "PETFOOD", "DF", "FB", "CALCIUM", "HC", "JELLY", "CAKE", "GCOFFEE", "BOILER", "PIE", "NOOD", "BREAD", "CERE", "CPOWDER", "LIQM", "TP", "COTTONPADS", "EDOIL", "INSR", "GUM", "INSE", "DEODORANT", "HAIR", "COLDC", "HAIRS", "COFF", "CPS", "DIAP", "RPM", "QSOUP", "TONIC", "LLP", "BATT", "CHEESE", "SD", "FT", "MOUTHWASH", "FD", "BFOOD", "BOUNI", "RWC", "INSB", "MOIS", "RWB", "AL", "BABYNUTRFOOD", "BABYP", "HERBALTEA", "LGELS", "CONDOM", "SHAVERS", "DTABLEWARE", "SOYSAUCE", "BLADE", "CSD", "ATD", "JUICE")
    val map = list.zipWithIndex.toMap
    map.getOrElse(s, 96)
  }

}
