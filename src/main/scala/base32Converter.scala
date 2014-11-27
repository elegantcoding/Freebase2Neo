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

object base32Converter {
 
  val mask = Integer.parseInt("11111", 2)
  val bits = 5
  val maxChars = (8 * 8) / bits
  var asciiToCodeLookup = Array.fill[Int](128)(-1)
  var codeToAsciiLookup = Array.fill[Char](32)('_')
  
  var code = 0
  ('0' to '9').foreach{c => asciiToCodeLookup(c)=code; codeToAsciiLookup(code)=c; code+=1}
  ('a' to 'z').foreach{c => if(!vowel(c)) {asciiToCodeLookup(c)=code; codeToAsciiLookup(code)=c; code+=1}}
  asciiToCodeLookup('_')=code
  codeToAsciiLookup(code)='_'

  private def vowel(c:Char) =
    (c == 'a' || c == 'e' || c == 'i' || c == 'o' || c == 'u')

  // 10s for 100M encodes... probably not worth optimizing
  def toDecimal(mid:String):Long = {
    var result = 0L
    val length = mid.length
    // throw exception if length greater than maxChars?
    (0 until length).foreach{i =>
      if (mid.charAt(i) >= 128) {
        // maybe throw exception?
        return -1
      }
      result |= asciiToCodeLookup(mid.charAt(i))
      if (i != length-1) result <<= bits
    }
    result
  }
 
  def toBase32(l:Long):String = {
    val result = Array.fill[Char](maxChars)(' ')
    var i=0
    var mid = l
    ((maxChars-1) to 0 by -1).foreach{x =>
      i = x
      result(i) = codeToAsciiLookup((mid & mask).toInt)
      mid = mid >> bits
      if (mid == 0) {
        return String.valueOf(result,i,maxChars-i)
      }
    } 
    String.valueOf(result,i,maxChars-i)
  }
}
