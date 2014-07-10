package com.elegantcoding.freebase2neo

import java.util.Arrays
import collection.mutable.BitSet

class IdMap {
  // TODO make this estimate based on file size?
  var arr = Array.fill[Long](200000000)(Long.MaxValue)
  var createdArr = BitSet.empty
  var idx:Int = 0
  var flag = false

  def putMid(mid:String) = {
    put(mid2long.encode(mid))
  }

  def containsMid(mid:String) = {
    contains(mid2long.encode(mid))
  }

  def getMid(mid:String) = {
    get(mid2long.encode(mid))
  }

  def setCreatedMid(mid:String) = {
    setCreated(mid2long.encode(mid))
  }

  def setCreated(id:Long) = {
    val i = get(id)
    createdArr.add(i)
  }

  def getCreatedMid(mid:String) = {
    getCreated(mid2long.encode(mid))
  }

  def getCreated(id:Long) = {
    val i = get(id)
    createdArr(i)
  }

  def put(mid:Long) = {
    flag = false
    arr(idx) = mid
    idx += 1
  }

  def get(mid:Long):Int = {
    if(!flag) throw new Exception("need to call done() before contains or get.")
    Arrays.binarySearch(arr, 0, idx, mid)
  }

  def contains(mid:Long):Boolean = {
    get(mid) >= 0
  }

  def done = {
    Arrays.sort(arr)
    // TODO make this estimate based on file size?
    val arr2 = Array.fill[Long](200000000)(Long.MaxValue)
    var lastx = Long.MinValue
    var i = 0
    (0 until idx).foreach{x =>
      if(arr(x) != lastx) {
        arr2(i) = arr(x)
        i += 1
      }
      lastx = arr(x)
    }
    idx = 0

    createdArr = BitSet(i)
    arr = Array.fill[Long](i)(Long.MaxValue)

    (0 until i).foreach{x =>
      arr(idx) = arr2(x)
      idx += 1
    }
    flag = true
  }

  def length:Int = {
    idx
  }
}