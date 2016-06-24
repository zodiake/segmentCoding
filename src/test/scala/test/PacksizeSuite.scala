package test

import org.scalatest.FunSuite
import com.nielsen.packsize.PacksizeCoding

class PacksizeSuite extends FunSuite {
  test("packsize1 test") {
    val desc = "燕麦乳250ml*6+1000g*2||产地:中国大陆||省份:其他"
    val a = PacksizeCoding.getPacksize(desc, "g")
    println(a)
  }
}