/**
 *    _____              _                    ____  _   _
 *   |  ___| __ ___  ___| |__   __ _ ___  ___|___ \| \ | | ___  ___
 *   | |_ | '__/ _ \/ _ \ '_ \ / _` / __|/ _ \ __) |  \| |/ _ \/ _ \
 *   |  _|| | |  __/  __/ |_) | (_| \__ \  __// __/| |\  |  __/ (_) |
 *   |_|  |_|  \___|\___|_.__/ \__,_|___/\___|_____|_| \_|\___|\___/
 *
 *
 * Copyright (c) 2013-2014
 *
 * Wes Freeman [http://wes.skeweredrook.com]
 * Geoff Moes [http://elegantcoding.com]
 *
 * FreeBase2Neo is designed to import Freebase RDF triple data into Neo4J
 *
 * FreeBase2Neo is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

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

      get(base32Converter.toDecimal(mid))
    }

    def length = midArray.length
  }

  var midArrayBuffer = ArrayBuffer[Long]()
  var currentIndex : Int = 0

  def putMid(mid:String) = {
    put(base32Converter.toDecimal(mid))
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
