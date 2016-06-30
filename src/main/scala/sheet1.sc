object sheet1 {
  val b = List(Array('a', 'b'), Array('a', 'c'), Array('b', 'c'))

  val c=b.groupBy(_ (0))
}