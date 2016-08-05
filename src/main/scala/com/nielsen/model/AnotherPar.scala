package com.nielsen.model

import java.util.concurrent.atomic.AtomicReference

/**
  * Created by wangqi08 on 18/7/2016.
  */

object Parsers {
  type Par[A] = String => Result[A]

  case class Id(id: String)

  trait Result[A]

  case class Success[Id](a: Id) extends Result[Id]

  class Fail[A] extends Result[A]


  val atomic=new AtomicReference[Int]()
  atomic.lazySet(1)
}

