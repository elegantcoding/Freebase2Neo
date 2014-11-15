package com.elegantcoding.freebase2neo.test

import com.elegantcoding.freebase2neo.{MidToIdMapBuilder}

import org.scalatest._

class idMapSpec extends FlatSpec with ShouldMatchers {

  "idMap" should "be able to add new mids" in {
    val midToIdMapBuilder = MidToIdMapBuilder()
    midToIdMapBuilder.putMid("hsdf")
  }

  it should "be able to add new ids" in {
    val midToIdMapBuilder = MidToIdMapBuilder()
    midToIdMapBuilder.put(123)
  }

  it should "be able to check if an id is contained" in {
    val midToIdMapBuilder = MidToIdMapBuilder()
    midToIdMapBuilder.put(123)

    val idMap = midToIdMapBuilder.getMidToIdMap
    idMap.get(123) should equal(0)
    idMap.get(1) should equal(-1)
    idMap.length should equal(1)
  }

  it should "be able to get the ids out of the map" in {
    val midToIdMapBuilder = MidToIdMapBuilder()
    midToIdMapBuilder.put(321)
    midToIdMapBuilder.put(123)
    val idMap = midToIdMapBuilder.getMidToIdMap
    idMap.get(123) should equal(0)
    idMap.get(321) should equal(1)
    idMap.length should equal(2)
  }

  it should "be able to sort/dedup" in {
    val midToIdMapBuilder = MidToIdMapBuilder()
    midToIdMapBuilder.put(321)
    midToIdMapBuilder.put(123)
    midToIdMapBuilder.put(123)
    val idMap = midToIdMapBuilder.getMidToIdMap
    idMap.length should equal(2)
    idMap.get(123) should equal(0)
    idMap.get(321) should equal(1)
  }
}
