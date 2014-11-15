package com.elegantcoding.freebase2neo

import java.util.Arrays
import scala.collection.mutable.{ArrayBuffer, BitSet}

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

      println("getMid : " + mid)
      println("getMid : mid2long.encode(mid) : " + mid2long.encode(mid))
      println("getMid : get(mid2long.encode(mid)) : " + get(mid2long.encode(mid)))

      get(mid2long.encode(mid))
    }

    def length = midArray.length

    //  def length:Int = {
    //    length
    //  }
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

//  def containsMid(mid:String) = {
//    contains(mid2long.encode(mid))
//  }
//
//
//
//  def contains(mid:Long):Boolean = {
//    get(mid) >= 0
//  }

  // convert ArrayBuffer to Array and sort
  def getMidToIdMap : MidToIdMap = {

    //scala.util.Sorting.stableSort()

    //var midArray = scala.util.Sorting.stableSort(midArrayBuffer.toList)
    var midArray = scala.util.Sorting.stableSort(midArrayBuffer.groupBy{x => x}.values.map(_.head).toList)

    //var midArray = scala.util.Sorting.stableSort(midArrayBuffer).groupBy{x => x}.map{_._2.head}.toArray
    //var midArray = scala.util.Sorting.stableSort(midArrayBuffer).toArray

    println("midArray:")
    println("midArray.length" + midArray.length)
    midArray.foreach{ println(_) }


    new MidToIdMapImpl(midArray)
  }
}

