package com.elegantcoding.freebase2neo

import com.googlecode.lanterna.TerminalFacade
import grizzled.slf4j.Logger
import java.nio.charset.Charset

package object Utils {
  val logger = Logger("com.elegantcoding.freebase2neo")
  var lastTime = System.currentTimeMillis
  val ONE_MILLION = 1000000
  val terminal = TerminalFacade.createTerminal(System.in, System.out, Charset.forName("UTF8"))
  terminal.enterPrivateMode
  terminal.setCursorVisible(false)

  def formatTime(elapsedTime: Long) = {
    "%02d:%02d:%02d".format(
      (elapsedTime / 1000) / 3600,
      ((elapsedTime / 1000) / 60) % 60,
      (elapsedTime / 1000) % 60)
  }

  def logStatus(processStartTime: Long, rdfLineCount: Long) = {
    val curTime = System.currentTimeMillis
    if (rdfLineCount % 100000 == 0) {
      terminal.moveCursor(10, 4)
      putString("%s elapsed".format(formatTime(curTime-processStartTime)))
      terminal.moveCursor(10, 5)
      putString("%dM lines read, at %dK lines/sec (avg.)  ".format(rdfLineCount/1000000, rdfLineCount/1000/((curTime-processStartTime)/1000)))
      terminal.moveCursor(10, 6)
      putString("%dK lines read, at %d lines/sec (avg.)  ".format(rdfLineCount/1000, rdfLineCount/((curTime-processStartTime)/1000)))
      terminal.moveCursor(10, 7)
      putString("%2.2f%% complete (approx.) ".format(rdfLineCount.toDouble/26200000L))
      terminal.moveCursor(10, 8)
      putString("%s time remaining (approx.)  ".format(formatTime((2620000000L-rdfLineCount)/(rdfLineCount/(curTime-processStartTime)))))
    }
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