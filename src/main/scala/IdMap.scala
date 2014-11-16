package com.elegantcoding.freebase2neo

import java.util.Arrays
import scala.collection.mutable.ArrayBuffer

trait MidToIdMap {
  val midArray: Array[Long]
  def length : Int
  def getMid(mid:String) : Int
  def get(mid:Long):Int
}

object MidToIdMapBuilder {
  def apply() = new MidToIdMapBuilder
}

class MidToIdMapBuilder {

  private class MidToIdMapImpl(val midArray : Array[Long]) extends MidToIdMap {

    def get(mid:Long) : Int = {
      Arrays.binarySearch(midArray, 0, midArray.length, mid)
    }

    def getMid(mid : String) : Int = {

      get(mid2long.encode(mid))
    }

    def length = midArray.length
  }

  var midArrayBuffer = ArrayBuffer[Long]()
  var currentIndex : Int = 0

  def putMid(mid:String) = {
    put(mid2long.encode(mid))
  }

  def put(mid:Long) = {

    midArrayBuffer += mid
    currentIndex += 1
  }

  def getMidToIdMap : MidToIdMap = {

    var midArray = scala.util.Sorting.stableSort(midArrayBuffer.groupBy{x => x}.values.map(_.head).toList)
    new MidToIdMapImpl(midArray)
  }
}

