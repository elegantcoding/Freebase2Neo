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

import com.elegantcoding.rdfprocessor.rdftriple.types.RdfTriple

import collection.JavaConverters._
import com.typesafe.config.{ConfigFactory, ConfigParseOptions, ConfigResolveOptions, ConfigSyntax}

//TODO: Convert this to YAML

class Settings(val fileName : String = "freebase2neo") {
  val config = ConfigFactory.load(
                 fileName,
                 ConfigParseOptions.defaults()
                   .setSyntax(ConfigSyntax.JSON)
                   .setAllowMissing(false),
                 ConfigResolveOptions.defaults()
                   .setUseSystemEnvironment(false))

  val freebaseRdfPrefix = config.getString("freebaseRdfPrefix")
  val outputGraphPath = config.getString("outputGraphPath")

  val nodeStoreMappedMemory = config.getString("nodeStoreMappedMemory")
  val relationshipStoreMappedMemory = config.getString("relationshipStoreMappedMemory")
  val propertyStoreMappedMemory = config.getString("propertyStoreMappedMemory")
  val propertyStoreStrings = config.getString("propertyStoreStrings")

  val gzippedNTripleFile = config.getString("gzippedNTripleFile")
  val errorLogFile = config.getString("errorLogFile")
  val statusLogFile = config.getString("statusLogFile")
  val nodeTypePredicates = getConfigList("nodeTypePredicates")


  //TODO move all of this filter functionality elsewhere

  def startsWithAny(s:String, l:Seq[String]):Boolean = {
    l.foreach(v => if(s.startsWith(v)) {true})
    false
  }

  def endsWithAny(s:String, l:Seq[String]):Boolean = {
    l.foreach(v => if(s.endsWith(v)) {true})
    false
  }

  def containsAny(s:String, l:Seq[String]):Boolean = {
    l.foreach(v => if(s.contains(v)) {true})
    false
  }

  case class RdfFilterSetting(
    startsWithSeq : Seq[String] = Seq[String](),
    endsWithSeq : Seq[String] = Seq[String](),
    containsSeq : Seq[String] = Seq[String](),
    equalsSeq : Seq[String] = Seq[String]()
  )
  case class RdfFilter(
    whitelist : RdfFilterSetting = RdfFilterSetting(),
    blacklist : RdfFilterSetting = RdfFilterSetting()
  )

  case class Filters(subjectFilter : RdfFilter, predicateFilter : RdfFilter, objectFilter : RdfFilter) {

    def matchFilter(string : String, filter : RdfFilter) = {

       (filter.whitelist.equalsSeq.contains(string) ||
        !filter.blacklist.equalsSeq.contains(string)) &&
       (startsWithAny(string, filter.whitelist.containsSeq) ||
        !startsWithAny(string, filter.blacklist.containsSeq)) &&
       (endsWithAny(string, filter.whitelist.endsWithSeq) ||
        !endsWithAny(string, filter.blacklist.endsWithSeq)) &&
       (containsAny(string, filter.whitelist.startsWithSeq) ||
        !containsAny(string, filter.blacklist.startsWithSeq))
    }

    def matchSubject(string : String) = {

      matchFilter(string, subjectFilter)
    }

    def matchPredicate(string : String) = {

      matchFilter(string, predicateFilter)
    }

    def matchObject(string : String) = {

      matchFilter(string, objectFilter)
    }

    def matchRdf(rdfTriple : RdfTriple) = {

        matchSubject(rdfTriple.subjectString) &&
        matchPredicate(rdfTriple.predicateString) &&
        matchObject(rdfTriple.objectString)
    }
  }


  def getConfigList(configString : String)  = {
    try {
      config.getList(configString).unwrapped.asScala.toSeq.map(_.asInstanceOf[String])
    } catch {
      case ex : Exception => Seq[String]()
    }
  }

  val filters = Filters(
    subjectFilter = RdfFilter(
      whitelist = RdfFilterSetting(
        startsWithSeq = getConfigList("filters.subject.whitelist.startsWith"),
        endsWithSeq = getConfigList("filters.subject.whitelist.endsWith"),
        containsSeq = getConfigList("filters.subject.whitelist.contains"),
        equalsSeq = getConfigList("filters.subject.whitelist.equals")),
      blacklist = RdfFilterSetting(
        startsWithSeq = getConfigList("filters.subject.blacklist.startsWith"),
        endsWithSeq = getConfigList("filters.subject.blacklist.endsWith"),
        containsSeq = getConfigList("filters.subject.blacklist.contains"),
        equalsSeq = getConfigList("filters.subject.blacklist.equals"))
    ),
    predicateFilter = RdfFilter(
      whitelist = RdfFilterSetting(
        startsWithSeq = getConfigList("filters.predicate.whitelist.startsWith"),
        endsWithSeq = getConfigList("filters.predicate.whitelist.endsWith"),
        containsSeq = getConfigList("filters.predicate.whitelist.contains"),
        equalsSeq = getConfigList("filters.predicate.whitelist.equals")
      ),
      blacklist = RdfFilterSetting(
        startsWithSeq = getConfigList("filters.predicate.blacklist.startsWith"),
        endsWithSeq = getConfigList("filters.predicate.blacklist.endsWith"),
        containsSeq = getConfigList("filters.predicate.blacklist.contains"),
        equalsSeq = getConfigList("filters.predicate.blacklist.equals")
      )
    ),
    objectFilter = RdfFilter(
      whitelist = RdfFilterSetting(
        startsWithSeq = getConfigList("filters.object.whitelist.startsWith"),
        endsWithSeq = getConfigList("filters.object.whitelist.endsWith"),
        containsSeq = getConfigList("filters.object.whitelist.contains"),
        equalsSeq = getConfigList("filters.object.whitelist.equals")
      ),
      blacklist = RdfFilterSetting(
        startsWithSeq = getConfigList("filters.object.blacklist.startsWith"),
        endsWithSeq = getConfigList("filters.object.blacklist.endsWith"),
        containsSeq = getConfigList("filters.object.blacklist.contains"),
        equalsSeq = getConfigList("filters.object.blacklist.equals")
      )
    )
  )
}
