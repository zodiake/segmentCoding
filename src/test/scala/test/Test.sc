package test

object Test {
  val a ="""\d*""".r
  val c = "asdf2123asdfa23asdf34"
  val e = a.findAllIn(c)
  println(e.next)
}