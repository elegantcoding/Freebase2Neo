package com.elegantcoding.freebase2neo

import java.util.Arrays

class IdMap {
  var arr = Array.fill[Long](100000000)(Long.MaxValue)
  var idx = 0
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

  def put(mid:Long) = {
    flag = false
    arr(idx) = mid
    idx += 1
  }

  def get(mid:Long):Long = {
    if(!flag) throw new Exception("need to call done() first.")
    Arrays.binarySearch(arr, 0, idx, mid).toLong
  }

  def contains(mid:Long):Boolean = {
    get(mid) >= 0
  }

  def done = {
    Arrays.sort(arr)
    flag = true
  }
}