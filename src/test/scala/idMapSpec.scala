package com.elegantcoding.freebase2neo.test

import com.elegantcoding.freebase2neo.IdMap

import org.scalatest._

class idMapSpec extends FlatSpec with ShouldMatchers {

  "idMap" should "be able to add new mids" in {
    val idMap = new IdMap()
    idMap.putMid("hsdf")
  }

  it should "be able to add new ids" in {
    val idMap = new IdMap()
    idMap.put(123)
  }

  it should "be able to check if an id is contained" in {
    val idMap = new IdMap()
    idMap.put(123)
    idMap.done
    idMap.contains(123) should equal(true)
    idMap.contains(1) should equal(false)
    idMap.length should equal(1)
  }

  it should "be able to get the ids out of the map" in {
    val idMap = new IdMap()
    idMap.put(321)
    idMap.put(123)
    idMap.done
    idMap.get(123) should equal(0)
    idMap.get(321) should equal(1)
    idMap.length should equal(2)
  }

  it should "be able to sort/dedup" in {
    val idMap = new IdMap()
    idMap.put(321)
    idMap.put(123)
    idMap.put(123)
    idMap.done
    idMap.length should equal(2)
    idMap.get(123) should equal(0)
    idMap.get(321) should equal(1)
  }

  it should "be able to mark as created" in {
    val idMap = new IdMap()
    idMap.put(321)
    idMap.put(123)
    idMap.put(123)
    idMap.done
    idMap.length should equal(2)
    idMap.get(123) should equal(0)
    idMap.get(321) should equal(1)
    idMap.createdArr.add(idMap.get(123))
    idMap.getCreated(123) should equal(true)
    idMap.getCreated(321) should equal(false)
    idMap.setCreated(321)
    idMap.getCreated(321) should equal(true)

  }

}
