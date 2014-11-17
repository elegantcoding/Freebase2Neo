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

import com.googlecode.lanterna.TerminalFacade
import java.nio.charset.Charset


//  _____ ___  ____   ___
// |_   _/ _ \|  _ \ / _ \
//   | || | | | | | | | | |
//   | || |_| | |_| | |_| |
//   |_| \___/|____/ \___/
//
//
//  Refactor to separate file (Status[Console].scala) and finish cleanup
//  Maybe move to RDF Processor

case class MovingAverage(name: String, interval: Long)

case class DisplayUnit(name: String, unit: Long)

object DisplayUnitMillion extends DisplayUnit("Million", 1000000l)

class ItemCountStatus(val name : String,
                      private val movingAveragesParam : Seq[MovingAverage] = Nil,
                      val displayUnit : DisplayUnit = DisplayUnitMillion) {

  val startTime = System.currentTimeMillis
  var count : Long = 0
  var lastAvgTime = System.currentTimeMillis

  def incCount = count = count + 1

  def countInUnit = count / displayUnit.unit

  // There must be a better way to do this: have this be both publicly accessible and an inner class

  val movingAverages = movingAveragesParam.map{ new MovingAverageImpl(_) }

  class MovingAverageImpl (private val movingAverage : MovingAverage) {

    val name = movingAverage.name
    val interval = movingAverage.interval

    private var movingAverageValues = Seq[(Long,Long)]((0,0))

    def latestMovingAvg : Double = {

      val elapsed = System.currentTimeMillis - startTime

      movingAverageValues ++= Seq((count, elapsed))

      while (movingAverageValues.head._2 < elapsed - (interval)) {
        movingAverageValues = movingAverageValues.tail
      }

      if(elapsed == movingAverageValues.head._2) {
        0.0
      } else {
        (count - movingAverageValues.head._1) / (elapsed - movingAverageValues.head._2) * 1000.0
      }
    }
  }

  def elapsed = {
    val elapsedTime = System.currentTimeMillis - startTime; if (0 == elapsedTime) 1l else elapsedTime
  }

  def avgRate = count / elapsed * 1000.0

  def getLastAvgTime = {

    val curTime = System.currentTimeMillis

    if (curTime - 1000 > lastAvgTime) {
        lastAvgTime = curTime
    }

    lastAvgTime
  }
}


//class StageStatusInfo(val stage : Int,
//                      val stageDescription : String,
//                      val startTime : Long = System.currentTimeMillis) {
//}


//object StatusInfo {
//
//  //putString("%.3fM %s/sec (10 second moving average)        ".format(movingAverage.latestMovingAvg / ONE_MILLION, itemCountStatus.name))
//  //putString("%.3fM %s/sec (10 min moving average)           ".format(longMovingAvg / ONE_MILLION, itemCountStatus.name))
//
//  StatusInfo() =  new
//}


class StatusInfo(val stage : Int,
                 val stageDescription : String,
                 val itemCountStatus : Seq[ItemCountStatus]) {

  val startTime = System.currentTimeMillis

  def formatTime(time : Long) = {
    "%02d:%02d:%02d".format(
      (time / 1000) / 3600,
      ((time / 1000) / 60) % 60,
      (time / 1000) % 60)
  }

  def elapsed = System.currentTimeMillis - startTime
  def elapsedString = formatTime(System.currentTimeMillis - startTime)

}


class StatusConsole(private val displayInterval : Long = 1000) {

  var lastDisplayTime = System.currentTimeMillis

  var ONE_MILLION = 1000000l

  var line = 2
  var col = 10
  val terminal = TerminalFacade.createTerminal(Charset.forName("UTF8"))
  terminal.enterPrivateMode
  clear
  terminal.setCursorVisible(false)

  def putString(str:String) = {
    terminal.moveCursor(10, line)
    line += 1
    str.foreach(c => terminal.putCharacter(c))
  }

  def clear = {
    terminal.clearScreen
    line = 1
    col = 10
    putString("press ctrl-C to quit")
  }

  def displayProgress(statusInfo : StatusInfo) : Unit = {

      if((System.currentTimeMillis - displayInterval) <=  lastDisplayTime)
        return

    lastDisplayTime = System.currentTimeMillis

    //logStatus(statusInfo.startTime, lines)


      line = statusInfo.stage * 4 - 2
      col = 10
      putString("stage %d (%s)...                               ".format(statusInfo.stage, statusInfo.stageDescription))
      putString("%s elapsed                                     ".format(statusInfo.elapsedString))

      statusInfo.itemCountStatus.foreach((itemCountStatus) => {

        putString("%.3f %s %s processed                            ".format(itemCountStatus.countInUnit.toDouble, itemCountStatus.displayUnit.name, itemCountStatus.name))
        putString("%.3f %s %s/sec (cumulative average)             ".format(itemCountStatus.avgRate / itemCountStatus.displayUnit.unit.toDouble, itemCountStatus.displayUnit.name, itemCountStatus.name))
        putString("%d %s                                          ".format(itemCountStatus.count, itemCountStatus.name))

        itemCountStatus.movingAverages.foreach( (movingAverage) =>

          putString("%.3f %s %s/sec %s        ".format(movingAverage.latestMovingAvg / itemCountStatus.displayUnit.unit.toDouble, itemCountStatus.displayUnit.name, itemCountStatus.name, movingAverage.name))
        )
      })
//      putString("%d %s                                          ".format(itemCount, itemDesc))
//      putString("%.3fM %s/sec (10 second moving average)        ".format(shortItemMovingAvg / ONE_MILLION, itemDesc))
      //putString("%2.2f%% complete (approx.)                     ".format(lines.toDouble / total * 100))
      //putString("%s time remaining (approx.)                    ".format(formatTime(((total - lines) / longMovingAvg * 1000).toLong)))
  }

  def displayDone(statusInfo : StatusInfo) = {

    line = statusInfo.stage * 4 - 2
    col = 10
    putString("stage %d (%s) complete. elapsed: %s                                   ".format(statusInfo.stage, statusInfo.stageDescription, statusInfo.elapsedString))

    statusInfo.itemCountStatus.foreach((itemCountStatus) => {

      putString("%d %s processed,                                                      ".format(itemCountStatus.count, itemCountStatus.name))
      //putString("%.3f %s %s/sec (average); %.3fM %s/sec (average)                       ".format(itemCountStatus.avgRate / ONE_MILLION, itemCountStatus.displayUnit.name, itemCountStatus.name))
      putString("%.3f %s %s/sec (average);                                             ".format(itemCountStatus.avgRate / ONE_MILLION, itemCountStatus.displayUnit.name, itemCountStatus.name))
    })

    putString("                                                                      ")
  }

  //def cleanupTerminal = terminal.exitPrivateMode()

  def checkForExit = {
    val key = terminal.readInput()
    if (key != null && key.isCtrlPressed && key.getCharacter == 'c') {
      //cleanupTerminal
      terminal.exitPrivateMode()
      System.exit(0)
    }
  }

  def logStatus(processStartTime: Long, rdfLineCount: Long) = {
    val curTime = System.currentTimeMillis
    checkForExit
    if (rdfLineCount % (ONE_MILLION * 10L) == 0) {
      // logger.info(": " + rdfLineCount / 1000000 + "M tripleString lines processed" +
      //   "; last 10M: " + formatTime(curTime - lastTime) +
      //   "; process elapsed: " + formatTime(curTime - processStartTime))
      //lastTime = curTime
    }
  }


}


package object Utils {
//  var lastTime = System.currentTimeMillis
//  val ONE_MILLION = 1000000l
//  var shortMovingAvgs = Seq[(Long,Long)]((0,0))
//  var longMovingAvgs = Seq[(Long,Long)]((0,0))
//  var shortItemMovingAvgs = Seq[(Long,Long)]((0,0))
//  var lastAvgTime = System.currentTimeMillis
//  var line = 2
//  var col = 10
//  val terminal = TerminalFacade.createTerminal(Charset.forName("UTF8"))
//  terminal.enterPrivateMode
//  clear
//  terminal.setCursorVisible(false)
//
//  def clear = {
//    terminal.clearScreen
//    line = 1
//    col = 10
//    putString("press ctrl-C to quit")
//  }


//  def latestShortMovingAvg(current:Long, elapsed:Long):Double = {
//    shortMovingAvgs ++= Seq((current,elapsed))
//    while (shortMovingAvgs.head._2 < elapsed - (10 * 1000)) {
//      shortMovingAvgs = shortMovingAvgs.tail
//    }
//    if(elapsed == shortMovingAvgs.head._2) {
//      0.0
//    } else {
//      (current - shortMovingAvgs.head._1) / (elapsed - shortMovingAvgs.head._2) * 1000.0
//    }
//  }
//
//  def latestLongMovingAvg(current:Long, elapsed:Long):Double = {
//    longMovingAvgs ++= Seq((current, elapsed))
//    while (longMovingAvgs.head._2 < elapsed - (10 * 60 * 1000)) {
//      longMovingAvgs = longMovingAvgs.tail
//    }
//    if (elapsed == longMovingAvgs.head._2) {
//      0.0
//    } else {
//      (current - longMovingAvgs.head._1) / (elapsed - longMovingAvgs.head._2) * 1000.0
//    }
//  }

//  def latestItemShortMovingAvg(current:Long, elapsed:Long):Double = {
//    shortItemMovingAvgs ++= Seq((current, elapsed))
//    while (shortItemMovingAvgs.head._2 < elapsed - (10 * 1000)) {
//      shortItemMovingAvgs = shortItemMovingAvgs.tail
//    }
//    if (elapsed == longMovingAvgs.head._2) {
//      0.0
//    } else {
//      (current - shortItemMovingAvgs.head._1) / (elapsed - shortItemMovingAvgs.head._2) * 1000.0
//    }
//  }

//  def displayProgress(stage:Int, desc:String, startTime: Long, total:Long, totalDesc:String, lines: Long, itemCount:Long, itemDesc:String) = {
//    val curTime = System.currentTimeMillis
//    if (curTime - 1000 > lastAvgTime) {
//      lastAvgTime = curTime
//      var elapsed = curTime - startTime
//      if (elapsed == 0) elapsed = 1
//      val avgRate:Double = lines / elapsed * 1000.0
//      //var shortMovingAvg:Double = latestShortMovingAvg(lines, elapsed)
//      //if (shortMovingAvg == 0) shortMovingAvg = 1
//      //var longMovingAvg:Double = latestLongMovingAvg(lines,elapsed)
//      //if (longMovingAvg == 0) longMovingAvg = 1
//      //var shortItemMovingAvg:Double = latestItemShortMovingAvg(itemCount, elapsed)
//      //if (shortItemMovingAvg == 0) shortItemMovingAvg = 1
//      logStatus(startTime, lines)
//      line = stage * 4 - 2
//      col = 10
//      putString("stage %d (%s)...                               ".format(stage, desc))
//      putString("%s elapsed                                     ".format(formatTime(elapsed)))
//      putString("%.3fM %s processed                             ".format(lines / ONE_MILLION.toDouble, totalDesc))
//      putString("%.3fM %s/sec (cumulative average)              ".format(avgRate / ONE_MILLION, totalDesc))
//      putString("%.3fM %s/sec (10 second moving average)        ".format(shortMovingAvg / ONE_MILLION, totalDesc))
//      putString("%.3fM %s/sec (10 min moving average)           ".format(longMovingAvg / ONE_MILLION, totalDesc))
//      putString("%d %s                                          ".format(itemCount, itemDesc))
//      putString("%.3fM %s/sec (10 second moving average)        ".format(shortItemMovingAvg / ONE_MILLION, itemDesc))
//      putString("%2.2f%% complete (approx.)                     ".format(lines.toDouble / total * 100))
//      putString("%s time remaining (approx.)                    ".format(formatTime(((total - lines) / longMovingAvg * 1000).toLong)))
//    }
//  }
//
//  def displayDone(stage:Int, desc:String, startTime: Long, total: Long, totalDesc:String, itemCount:Long, itemDesc:String) = {
//    val curTime = System.currentTimeMillis
//    var elapsed = curTime - startTime
//    if (elapsed == 0) elapsed = 1
//    val avgRate:Double = total / elapsed * 1000.0
//    val itemAvg:Double = itemCount / elapsed * 1000.0
//    line = stage * 4 - 2
//    col = 10
//    putString("stage %d (%s) complete. elapsed: %s                                   ".format(stage, desc, formatTime(elapsed)))
//    putString("%d %s processed, %d %s                                                ".format(total, totalDesc, itemCount, itemDesc))
//    putString("%.3fM %s/sec (average); %.3fM %s/sec (average)                        ".format(avgRate / ONE_MILLION, totalDesc, itemAvg / ONE_MILLION, itemDesc))
//    putString("                                                                      ")
//    shortMovingAvgs = Seq[(Long,Long)]((0,0))
//    longMovingAvgs = Seq[(Long,Long)]((0,0))
//    shortItemMovingAvgs = Seq[(Long,Long)]((0,0))
//  }

//  def logStatus(processStartTime: Long, rdfLineCount: Long) = {
//    val curTime = System.currentTimeMillis
//    checkForExit
//    if (rdfLineCount % (ONE_MILLION * 10L) == 0) {
//     // logger.info(": " + rdfLineCount / 1000000 + "M tripleString lines processed" +
//     //   "; last 10M: " + formatTime(curTime - lastTime) +
//     //   "; process elapsed: " + formatTime(curTime - processStartTime))
//      lastTime = curTime
//    }
//  }
//
//  def checkForExit = {
//    val key = terminal.readInput()
//    if (key != null && key.isCtrlPressed && key.getCharacter == 'c') {
//      cleanupTerminal
//      System.exit(0)
//    }
//  }

//  def putString(str:String) = {
//    terminal.moveCursor(10, line)
//    line += 1
//    str.foreach(c => terminal.putCharacter(c))
//  }

//  val idStart = "<http://rdf.freebase.com/ns/m.".length
//  def extractId(str:String):Long = {
//    // println(str + " ==> " + str.substring(idStart, str.length()-1))
//    mid2long.encode(str.substring(idStart, str.length()-1))
//  }

//  def cleanupTerminal = terminal.exitPrivateMode()


}
