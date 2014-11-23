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

class StatusInfo(val stage : Int,
                 val stageDescription : String,
                 val itemCountStatus : Seq[ItemCountStatus]) {

  val startTime = System.currentTimeMillis

  def formatTime(time : Long) = {
    "%02d:%02d:%02d".format(
      time / (1000 * 60 * 60),
      (time / (1000 * 60)) % 60,
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
        putString("%d %s                                           ".format(itemCountStatus.count, itemCountStatus.name))

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

