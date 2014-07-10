package com.elegantcoding.freebase2neo

import com.googlecode.lanterna.TerminalFacade
import grizzled.slf4j.Logger
import java.nio.charset.Charset

package object Utils {
  val logger = Logger("com.elegantcoding.freebase2neo")
  var lastTime = System.currentTimeMillis
  val ONE_MILLION = 1000000l
  var shortMovingAvgs = Seq[Double]()
  var longMovingAvgs = Seq[Double]()
  var shortItemMovingAvgs = Seq[Double]()
  var lastAvgTime = System.currentTimeMillis
  var lastAvgLines = 1000l
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

  def logProgress(stage:Int, desc:String, start: Long, total:Long, lines: Long, itemCount:Long, itemDesc:String) = {
    val curTime = System.currentTimeMillis
    if (curTime - 1000 > lastAvgTime) {
      var elapsed = curTime - start
      if (elapsed == 0) elapsed = 1
      val millions:Long = lines / ONE_MILLION
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
      lastAvgItems = itemCount
      var shortItemMovingAvg:Double = latestItemShortMovingAvg(secondAvg)
      if (shortItemMovingAvg == 0) shortItemMovingAvg = 1
      logStatus(start, lines)
      line = 4
      col = 10
      putString("stage %d (%s)...".format(stage))
      putString("%s elapsed    ".format(formatTime(elapsed)))
      putString("%dM triples read    ".format(millions))
      putString("%.3fM triples/sec (cumulative average)     ".format(avgRate / ONE_MILLION))
      putString("%.3fM triples/sec (10 second moving average)     ".format(shortMovingAvg / ONE_MILLION))
      putString("%.3fM triples/sec (10 min moving average)     ".format(longMovingAvg / ONE_MILLION))
      putString("%d %s       ".format(itemCount, itemDesc))
      putString("%.3fM %s/sec (10 second moving average)     ".format(itemShortMovingAvg / ONE_MILLION))
      putString("%2.2f%% complete (approx.)             ".format(lines.toDouble / total * 100))
      putString("%s time remaining (approx.)               ".format(formatTime(((total - lines) / longMovingAvg * 1000).toLong)))
    }
  }

  def logDone(stage:Int, desc:String, processStartTime: Long, lines: Long, ids:Long) = {
    val curTime = System.currentTimeMillis
    var elapsed = curTime - processStartTime
    if (elapsed == 0) elapsed = 1
    val millions:Long = lines / ONE_MILLION
    val avgRate:Double = lines / elapsed * 1000.0
    clear
    line = 4
    col = 10
    putString("first pass (collecting machine ids)...")
    putString("%s elapsed  ".format(formatTime(elapsed)))
    putString("%dM triples read  ".format(millions))
    putString("%.3fM triples/sec (average)     ".format(avgRate / ONE_MILLION))
    putString("%d machine ids collected".format(ids))
    putString("finished                                ")
  }

  def logSecondPass(processStartTime: Long, lines: Long, nodes:Long, rels:Long) = {
    val curTime = System.currentTimeMillis
    if (curTime - 1000 > lastAvgTime) {
      var elapsed = curTime - processStartTime
      if (elapsed == 0) elapsed = 1
      val millions:Long = lines / ONE_MILLION
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
      val total:Long = 2630 * ONE_MILLION
      logStatus(processStartTime, lines)
      line = 11
      col = 10
      putString("second pass (creating nodes and relationships)...")
      putString("%s elapsed    ".format(formatTime(elapsed)))
      putString("%dM triples read        ".format(millions))
      putString("%.3fM triples/sec (cumulative average)     ".format(avgRate / ONE_MILLION))
      putString("%.3fM triples/sec (10 second moving average)     ".format(shortMovingAvg / ONE_MILLION))
      putString("%.3fM triples/sec (10 min moving average)     ".format(longMovingAvg / ONE_MILLION))
      putString("%d nodes created       ".format(nodes))
      putString("%d relationships created       ".format(rels))
      putString("%2.2f%% complete (approx.)             ".format(lines.toDouble / total * 100))
      putString("%s time remaining (approx.)               ".format(formatTime(((total - lines) / longMovingAvg * 1000).toLong)))
    }
  }

  def logSecondPassDone(processStartTime: Long, lines: Long, nodes:Long, rels:Long) = {
    val curTime = System.currentTimeMillis
    var elapsed = curTime - processStartTime
    if (elapsed == 0) elapsed = 1
    val millions:Long = lines / ONE_MILLION
    val avgRate:Double = lines / elapsed * 1000.0
    line = 11
    col = 10
    putString("second pass (creating nodes and relationships)...")
    putString("%s elapsed  ".format(formatTime(elapsed)))
    putString("%dM triples read  ".format(millions))
    putString("%.3fM triples/sec (average)     ".format(avgRate / ONE_MILLION))
    putString("%d nodes created".format(nodes))
    putString("%d relationships created".format(rels))
    putString("finished                                ")
  }

  def logStatus(processStartTime: Long, rdfLineCount: Long) = {
    val curTime = System.currentTimeMillis
    checkForExit
    if (rdfLineCount % (ONE_MILLION * 10L) == 0) {
      logger.info(": " + rdfLineCount / 1000000 + "M tripleString lines processed" +
        "; last 10M: " + formatTime(curTime - lastTime) +
        "; process elapsed: " + formatTime(curTime - processStartTime))
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