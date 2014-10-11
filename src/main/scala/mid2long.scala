package com.elegantcoding.freebase2neo

object mid2long {
 
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

  def vowel(c:Char) = 
    (c == 'a' || c == 'e' || c == 'i' || c == 'o' || c == 'u')

  // 10s for 100M encodes... probably not worth optimizing
  def encode(mid:String):Long = {
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
 
  def decode(l:Long):String = {
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
