object sheet1 {
  val list = List("1", "2", "23", "13")

  trait FilterSub

  for {
    j <- list
    k <- list.filter(_ != j) if (k.indexOf(j) > -1 && k.length > j.length)
  } yield
    k
}