package com.elegantcoding.freebase2neo

import com.googlecode.lanterna.TerminalFacade
import grizzled.slf4j.Logger
import java.nio.charset.Charset

package object Utils {
  val logger = Logger("com.elegantcoding.freebase2neo")
  var lastTime = System.currentTimeMillis
  val ONE_MILLION = 1000000
  val terminal = TerminalFacade.createTerminal(Charset.forName("UTF8"))
  terminal.enterPrivateMode
  terminal.clearScreen
  terminal.setCursorVisible(false)
  terminal.moveCursor(10, 2)
  putString("press ctrl-C to quit")

  def formatTime(elapsedTime: Long) = {
    "%02d:%02d:%02d".format(
      (elapsedTime / 1000) / 3600,
      ((elapsedTime / 1000) / 60) % 60,
      (elapsedTime / 1000) % 60)
  }

  def logFirstPass(processStartTime: Long, rdfLineCount: Long) = {
    var lines = rdfLineCount
    val curTime = System.currentTimeMillis
    if (rdfLineCount % 100000 == 0) {
      logStatus(processStartTime, rdfLineCount)
      terminal.moveCursor(10, 5)
      putString("first pass (collecting machine ids)...")
      terminal.moveCursor(10, 6)
      putString("%s elapsed".format(formatTime(curTime - processStartTime)))
      terminal.moveCursor(10, 7)
      if (lines == 0) lines += 1
      val thousands:Long = lines / 1000
      val millions:Long = lines / 1000000
      val avgKRate:Double = thousands / ((curTime - processStartTime) / 1000)
      val total = 2630000000L
      putString("%dM lines read".format(millions))
      terminal.moveCursor(10, 8)
      putString("%.0fK lines/sec (avg.)  ".format(avgKRate))
      terminal.moveCursor(10, 9)
      putString("%2.2f%% complete (approx.) ".format(lines.toDouble / total * 100))
      terminal.moveCursor(10, 10)
      putString("%s time remaining (approx.)                   ".format(formatTime(((total - lines) / avgKRate).toLong)))
    }
  }

  def logStatus(processStartTime: Long, rdfLineCount: Long) = {
    val curTime = System.currentTimeMillis
    val key = terminal.readInput()
    if (key != null && key.isCtrlPressed && key.getCharacter == 'c') System.exit(0)
    if (rdfLineCount % (ONE_MILLION * 10L) == 0) {
      logger.info(": " + rdfLineCount / 1000000 + "M tripleString lines processed" +
        "; last 10M: " + formatTime(curTime - lastTime) +
        "; process elapsed: " + formatTime(curTime - processStartTime))
      lastTime = curTime
    }
  }

  def putString(str:String) = {
    str.foreach(c => terminal.putCharacter(c))
  }

  def extractId(str:String):Long = {
    mid2long.encode(str.substring(31, str.length()-1))
  }

  def cleanupTerminal = terminal.exitPrivateMode()
}