package com.elegantcoding.freebase2neo

import com.googlecode.lanterna.TerminalFacade
import java.nio.charset.Charset

package object Utils {
  var lastTime = System.currentTimeMillis
  val ONE_MILLION = 1000000l
  var shortMovingAvgs = Seq[Double]()
  var longMovingAvgs = Seq[Double]()
  var shortItemMovingAvgs = Seq[Double]()
  var lastAvgTime = System.currentTimeMillis
  var lastAvgLines = 1000l
  var lastAvgItems = 1000l
  var line = 2
  var col = 10
  val terminal = TerminalFacade.createTerminal(Charset.forName("UTF8"))
  terminal.enterPrivateMode
  clear
  terminal.setCursorVisible(false)

  def clear = {
    terminal.clearScreen
    line = 2
    col = 10
    putString("press ctrl-C to quit")
  }

  def formatTime(elapsedTime: Long) = {
    "%02d:%02d:%02d".format(
      (elapsedTime / 1000) / 3600,
      ((elapsedTime / 1000) / 60) % 60,
      (elapsedTime / 1000) % 60)
  }

  def latestShortMovingAvg(avgKRate:Double):Double = {
    shortMovingAvgs ++= Seq(avgKRate)
    while (shortMovingAvgs.size > 10) {
      shortMovingAvgs = shortMovingAvgs.tail
    }
    shortMovingAvgs.reduce(_ + _) / shortMovingAvgs.size.toDouble
  }

  def latestLongMovingAvg(avgKRate:Double):Double = {
    longMovingAvgs ++= Seq(avgKRate)
    while (longMovingAvgs.size > 10 * 60) {
      longMovingAvgs = longMovingAvgs.tail
    }
    longMovingAvgs.reduce(_ + _) / longMovingAvgs.size.toDouble
  }

  def latestItemShortMovingAvg(avgKRate:Double):Double = {
    shortItemMovingAvgs ++= Seq(avgKRate)
    while (shortItemMovingAvgs.size > 10) {
      shortItemMovingAvgs = shortItemMovingAvgs.tail
    }
    shortItemMovingAvgs.reduce(_ + _) / shortItemMovingAvgs.size.toDouble
  }

  def displayProgress(stage:Int, desc:String, startTime: Long, total:Long, totalDesc:String, lines: Long, itemCount:Long, itemDesc:String) = {
    val curTime = System.currentTimeMillis
    if (curTime - 1000 > lastAvgTime) {
      var elapsed = curTime - startTime
      if (elapsed == 0) elapsed = 1
      val avgRate:Double = lines / elapsed * 1000.0
      var elapsedAvg = curTime - lastAvgTime
      if (elapsedAvg == 0) elapsedAvg = 1
      val secondAvg:Double = (lines - lastAvgLines) / elapsedAvg * 1000.0
      lastAvgLines = lines
      lastAvgTime = System.currentTimeMillis
      var shortMovingAvg:Double = latestShortMovingAvg(secondAvg)
      if (shortMovingAvg == 0) shortMovingAvg = 1
      var longMovingAvg:Double = latestLongMovingAvg(secondAvg)
      if (longMovingAvg == 0) longMovingAvg = 1
      val secondItemAvg:Double = (itemCount - lastAvgItems) / elapsedAvg * 1000.0
      lastAvgItems = itemCount
      var shortItemMovingAvg:Double = latestItemShortMovingAvg(secondItemAvg)
      if (shortItemMovingAvg == 0) shortItemMovingAvg = 1
      logStatus(startTime, lines)
      line = stage * 4 - 2
      col = 10
      putString("stage %d (%s)...                               ".format(stage, desc))
      putString("%s elapsed                                     ".format(formatTime(elapsed)))
      putString("%.3fM %s processed                             ".format(lines / ONE_MILLION.toDouble, totalDesc))
      putString("%.3fM %s/sec (cumulative average)              ".format(avgRate / ONE_MILLION, totalDesc))
      putString("%.3fM %s/sec (10 second moving average)        ".format(shortMovingAvg / ONE_MILLION, totalDesc))
      putString("%.3fM %s/sec (10 min moving average)           ".format(longMovingAvg / ONE_MILLION, totalDesc))
      putString("%d %s                                          ".format(itemCount, itemDesc))
      putString("%.3fM %s/sec (10 second moving average)        ".format(shortItemMovingAvg / ONE_MILLION, itemDesc))
      putString("%2.2f%% complete (approx.)                     ".format(lines.toDouble / total * 100))
      putString("%s time remaining (approx.)                     ".format(formatTime(((total - lines) / longMovingAvg * 1000).toLong)))
    }
  }

  def displayDone(stage:Int, desc:String, startTime: Long, total: Long, totalDesc:String, itemCount:Long, itemDesc:String) = {
    val curTime = System.currentTimeMillis
    var elapsed = curTime - startTime
    if (elapsed == 0) elapsed = 1
    val avgRate:Double = total / elapsed * 1000.0
    val itemAvg:Double = itemCount / elapsed * 1000.0
    line = stage * 4 - 2
    col = 10
    putString("stage %d (%s) complete. elapsed: %s                                   ".format(stage, desc, formatTime(elapsed)))
    putString("%d %s processed, %d %s                                                ".format(total, totalDesc, itemCount, itemDesc))
    putString("%.3fM %s/sec (average); %.3fM %s/sec (average)                        ".format(avgRate / ONE_MILLION, totalDesc, itemAvg / ONE_MILLION, itemDesc))
    putString("                                                                      ")
  }

  def logStatus(processStartTime: Long, rdfLineCount: Long) = {
    val curTime = System.currentTimeMillis
    checkForExit
    if (rdfLineCount % (ONE_MILLION * 10L) == 0) {
     // logger.info(": " + rdfLineCount / 1000000 + "M tripleString lines processed" +
     //   "; last 10M: " + formatTime(curTime - lastTime) +
     //   "; process elapsed: " + formatTime(curTime - processStartTime))
      lastTime = curTime
    }
  }

  def checkForExit = {
    val key = terminal.readInput()
    if (key != null && key.isCtrlPressed && key.getCharacter == 'c') {
      cleanupTerminal
      System.exit(0)
    }
  }

  def putString(str:String) = {
    terminal.moveCursor(10, line)
    line += 1
    str.foreach(c => terminal.putCharacter(c))
  }

  def extractId(str:String):Long = {
    mid2long.encode(str.substring(31, str.length()-1))
  }

  def cleanupTerminal = terminal.exitPrivateMode()
}