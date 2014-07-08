package com.elegantcoding.freebase2neo

import com.googlecode.lanterna.TerminalFacade
import grizzled.slf4j.Logger
import java.nio.charset.Charset

package object Utils {
  val logger = Logger("com.elegantcoding.freebase2neo")
  var lastTime = System.currentTimeMillis
  val ONE_MILLION = 1000000l
  var bufferedAvgs = Seq[Double]()
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
    putString("press ctrl-C to quit")
  }

  def formatTime(elapsedTime: Long) = {
    "%02d:%02d:%02d".format(
      (elapsedTime / 1000) / 3600,
      ((elapsedTime / 1000) / 60) % 60,
      (elapsedTime / 1000) % 60)
  }

  def addToBufferedAvgs(avgKRate:Double):Double = {
    bufferedAvgs ++= Seq(avgKRate)
    if (bufferedAvgs.size > 100) {
      bufferedAvgs = bufferedAvgs.tail
    }
    bufferedAvgs.reduce(_ + _) / bufferedAvgs.size.toDouble
  }

  def logFirstPass(processStartTime: Long, lines: Long, ids:Long) = {
    val curTime = System.currentTimeMillis
    if (lines % ONE_MILLION == 0 && lines != 0) {
      var elapsed = curTime - processStartTime
      if (elapsed == 0) elapsed = 1
      val millions:Long = lines / ONE_MILLION
      val avgRate:Double = lines / elapsed * 1000.0
      var elapsedAvg = curTime - lastAvgTime
      if (elapsedAvg == 0) elapsedAvg = 1
      val movingAvg:Double = (lines - lastAvgLines) / elapsedAvg * 1000.0
      lastAvgLines = lines
      lastAvgTime = System.currentTimeMillis
      var bufferedAvg = addToBufferedAvgs(movingAvg)
      if (bufferedAvg == 0) bufferedAvg = 1
      val total:Long = 2630 * ONE_MILLION
      logStatus(processStartTime, lines)
      line = 4
      col = 10
      putString("first pass (collecting machine ids)...")
      putString("%s elapsed    ".format(formatTime(elapsed)))
      putString("%dM triples read    ".format(millions))
      putString("%.0fK triples/sec (cumulative average)     ".format(avgRate / 1000))
      putString("%.0fK triples/sec (1M moving average)     ".format(movingAvg / 1000))
      putString("%.0fK triples/sec (100M moving average)     ".format(bufferedAvg / 1000))
      putString("%d machine ids collected       ".format(ids))
      putString("%2.2f%% complete (approx.)             ".format(lines.toDouble / total * 100))
      putString("%s time remaining (approx.)               ".format(formatTime(((total - lines) / bufferedAvg * 1000).toLong)))
    }
  }

  def logFirstPassDone(processStartTime: Long, lines: Long, ids:Long) = {
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
    putString("%.0fK triples/sec (average)     ".format(avgRate / 1000))
    putString("%d machine ids collected".format(ids))
    putString("finished                                ")
    bufferedAvgs = Seq[Double]()
  }

  def logSecondPass(processStartTime: Long, lines: Long, nodes:Long, rels:Long) = {
    val curTime = System.currentTimeMillis
    if (lines % ONE_MILLION == 0 && lines != 0) {
      var elapsed = curTime - processStartTime
      if (elapsed == 0) elapsed = 1
      val millions:Long = lines / ONE_MILLION
      val avgRate:Double = lines / elapsed * 1000.0
      var elapsedAvg = curTime - lastAvgTime
      if (elapsedAvg == 0) elapsedAvg = 1
      val movingAvg:Double = (lines - lastAvgLines) / elapsedAvg * 1000.0
      lastAvgLines = lines
      lastAvgTime = System.currentTimeMillis
      var bufferedAvg = addToBufferedAvgs(movingAvg)
      if (bufferedAvg == 0) bufferedAvg = 1
      val total:Long = 2630 * ONE_MILLION
      logStatus(processStartTime, lines)
      line = 11
      col = 10
      putString("second pass (creating nodes and relationships)...")
      putString("%s elapsed    ".format(formatTime(elapsed)))
      putString("%dM triples read        ".format(millions))
      putString("%.0fK triples/sec (cumulative average)     ".format(avgRate / 1000))
      putString("%.0fK triples/sec (1M moving average)     ".format(movingAvg / 1000))
      putString("%.0fK triples/sec (100M moving average)     ".format(bufferedAvg / 1000))
      putString("%d nodes created       ".format(nodes))
      putString("%d relationships created       ".format(rels))
      putString("%2.2f%% complete (approx.)             ".format(lines.toDouble / total * 100))
      putString("%s time remaining (approx.)               ".format(formatTime(((total - lines) / bufferedAvg * 1000).toLong)))
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
    putString("%.0fK triples/sec (average)     ".format(avgRate / 1000))
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