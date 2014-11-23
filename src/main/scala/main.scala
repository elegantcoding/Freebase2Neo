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

import collection.JavaConverters._

import com.typesafe.scalalogging.slf4j.Logger
import org.slf4j.LoggerFactory
import org.neo4j.unsafe.batchinsert.BatchInserters

object Main extends App {
  val settings = new Settings()
  var logger = Logger(LoggerFactory.getLogger("freebase2neo.main"))
  val inserter = BatchInserters.inserter(
    settings.outputGraphPath,
    Map[String,String](
      "neostore.nodestore.db.mapped_memory" ->         settings.nodeStoreMappedMemory,
      "neostore.relationshipstore.db.mapped_memory" -> settings.relationshipStoreMappedMemory,
      "neostore.propertystore.db.mapped_memory" ->     settings.propertyStoreMappedMemory,
      "neostore.propertystore.db.strings" ->           settings.propertyStoreStrings
    ).asJava
  )
  val freebase2neo = new Freebase2Neo(inserter, settings)
  freebase2neo.createDb
  logger.info("done!")
}
