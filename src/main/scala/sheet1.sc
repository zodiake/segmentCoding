object sheet1 {
  def test(s: String, key: String, previousLength: Int = 0, result: List[Int] = Nil): List[Int] = {
    if (s.indexOf(key) > -1) {
      val a = s.drop(s.indexOf(key) + 1)
      val length = s.indexOf(key) + previousLength + 1
      test(a, key, length, s.indexOf(key) + previousLength :: result)
    } else
      result
  }                                               //> test: (s: String, key: String, previousLength: Int, result: List[Int])List[I
                                                  //| nt]

  test("agbgcgdg", "g")                           //> res0: List[Int] = List(7, 5, 3, 1)

  "abc".substring(0, "abc".length - 1)            //> res1: String = ab

  val plusString = "\\d+\\+".r                    //> plusString  : scala.util.matching.Regex = \d+\+

  plusString.pattern.matcher("123+").matches      //> res2: Boolean = true

}