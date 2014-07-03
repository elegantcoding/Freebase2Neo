package com.elegantcoding.freebase2neo.test

import com.elegantcoding.freebase2neo.IdMap

import org.scalatest._

class idMapSpec extends FlatSpec with ShouldMatchers {

  val idMap = new IdMap()

  "idMap" should "be able to add new ids" in {
    idMap.putMid("hsdf")
  }

}
