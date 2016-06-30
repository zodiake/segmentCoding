package packsize

import org.scalatest.FunSuite

/**
  * Created by wangqi08 on 29/6/2016.
  */
class PackSuite extends FunSuite {
  val regex ="""(\p{L}+)(\d+)(g)(\*)(\d+)([杯包盒])(\p{L}*)""".r

  test("""数字'+'UOM'+'*'+'数字'+'杯'""") {
    val res = "光明莫斯利安巴氏杀菌常温酸牛奶110g*18杯礼盒装" match {
      case regex(_, num, pack, op, i, size, _) => (num, pack, op, i, size)
    }
    assert(res ==("110", "g", "*", "18", "杯"))
  }

}
