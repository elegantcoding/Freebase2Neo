package com.elegantcoding.freebase2neo

import collection.JavaConverters._
import com.typesafe.config._

import java.util.ArrayList

object Settings {
  val config = ConfigFactory.load(
                 "freebase2neo",
                 ConfigParseOptions.defaults()
                   .setSyntax(ConfigSyntax.JSON)
                   .setAllowMissing(false),
                 ConfigResolveOptions.defaults()
                   .setUseSystemEnvironment(false))

  val freebaseRdfPrefix = config.getString("freebaseRdfPrefix")
  val outputGraphPath = config.getString("outputGraphPath")
  val gzippedNTripleFile = config.getString("gzippedNTripleFile")
  val errorLogFile = config.getString("errorLogFile")
  val statusLogFile = config.getString("statusLogFile")
  val nodeTypePredicates = config.getList("nodeTypePredicates").unwrapped.asScala.toSeq.map(_.asInstanceOf[String])

  case class RdfFilterSetting(
    startsWith:Seq[String] = Seq[String](),
    endsWith:Seq[String] = Seq[String](),
    contains:Seq[String] = Seq[String](),
    equals:Seq[String] = Seq[String]()
  )
  case class RdfFilter(
    whitelist:RdfFilterSetting = RdfFilterSetting(),
    blacklist:RdfFilterSetting = RdfFilterSetting()
  )
  case class Filters(subj:RdfFilter, pred:RdfFilter, obj:RdfFilter)

  val filters = Filters(
    subj = RdfFilter(
      whitelist = RdfFilterSetting(
        startsWith = try {
          config.getList("filters.subject.whitelist.startsWith").unwrapped.asScala.toSeq.map(_.asInstanceOf[String])
        } catch {
          case ex:Exception => Seq[String]()
        },
        endsWith = try {
          config.getList("filters.subject.whitelist.endsWith").unwrapped.asScala.toSeq.map(_.asInstanceOf[String])
        } catch {
          case ex:Exception => Seq[String]()
        },
        contains = try {
          config.getList("filters.subject.whitelist.contains").unwrapped.asScala.toSeq.map(_.asInstanceOf[String])
        } catch {
          case ex:Exception => Seq[String]()
        },
        equals = try {
          config.getList("filters.subject.whitelist.equals").unwrapped.asScala.toSeq.map(_.asInstanceOf[String])
        } catch {
          case ex:Exception => Seq[String]()
        }
      ),
      blacklist = RdfFilterSetting(
        startsWith = try {
          config.getList("filters.subject.blacklist.startsWith").unwrapped.asScala.toSeq.map(_.asInstanceOf[String])
        } catch {
          case ex:Exception => Seq[String]()
        },
        endsWith = try {
          config.getList("filters.subject.blacklist.endsWith").unwrapped.asScala.toSeq.map(_.asInstanceOf[String])
        } catch {
          case ex:Exception => Seq[String]()
        },
        contains = try {
          config.getList("filters.subject.blacklist.contains").unwrapped.asScala.toSeq.map(_.asInstanceOf[String])
        } catch {
          case ex:Exception => Seq[String]()
        },
        equals = try {
          config.getList("filters.subject.blacklist.equals").unwrapped.asScala.toSeq.map(_.asInstanceOf[String])
        } catch {
          case ex:Exception => Seq[String]()
        }
      )
    ),
    pred = RdfFilter(
      whitelist = RdfFilterSetting(
        startsWith = try {
          config.getList("filters.predicate.whitelist.startsWith").unwrapped.asScala.toSeq.map(_.asInstanceOf[String])
        } catch {
          case ex:Exception => Seq[String]()
        },
        endsWith = try {
          config.getList("filters.predicate.whitelist.endsWith").unwrapped.asScala.toSeq.map(_.asInstanceOf[String])
        } catch {
          case ex:Exception => Seq[String]()
        },
        contains = try {
          config.getList("filters.predicate.whitelist.contains").unwrapped.asScala.toSeq.map(_.asInstanceOf[String])
        } catch {
          case ex:Exception => Seq[String]()
        },
        equals = try {
          config.getList("filters.predicate.whitelist.equals").unwrapped.asScala.toSeq.map(_.asInstanceOf[String])
        } catch {
          case ex:Exception => Seq[String]()
        }
      ),
      blacklist = RdfFilterSetting(
        startsWith = try {
          config.getList("filters.predicate.blacklist.startsWith").unwrapped.asScala.toSeq.map(_.asInstanceOf[String])
        } catch {
          case ex:Exception => Seq[String]()
        },
        endsWith = try {
          config.getList("filters.predicate.blacklist.endsWith").unwrapped.asScala.toSeq.map(_.asInstanceOf[String])
        } catch {
          case ex:Exception => Seq[String]()
        },
        contains = try {
          config.getList("filters.predicate.blacklist.contains").unwrapped.asScala.toSeq.map(_.asInstanceOf[String])
        } catch {
          case ex:Exception => Seq[String]()
        },
        equals = try {
          config.getList("filters.predicate.blacklist.equals").unwrapped.asScala.toSeq.map(_.asInstanceOf[String])
        } catch {
          case ex:Exception => Seq[String]()
        }
      )
    ),
    obj = RdfFilter(
      whitelist = RdfFilterSetting(
        startsWith = try {
          config.getList("filters.object.whitelist.startsWith").unwrapped.asScala.toSeq.map(_.asInstanceOf[String])
        } catch {
          case ex:Exception => Seq[String]()
        },
        endsWith = try {
          config.getList("filters.object.whitelist.endsWith").unwrapped.asScala.toSeq.map(_.asInstanceOf[String])
        } catch {
          case ex:Exception => Seq[String]()
        },
        contains = try {
          config.getList("filters.object.whitelist.contains").unwrapped.asScala.toSeq.map(_.asInstanceOf[String])
        } catch {
          case ex:Exception => Seq[String]()
        },
        equals = try {
          config.getList("filters.object.whitelist.equals").unwrapped.asScala.toSeq.map(_.asInstanceOf[String])
        } catch {
          case ex:Exception => Seq[String]()
        }
      ),
      blacklist = RdfFilterSetting(
        startsWith = try {
          config.getList("filters.object.blacklist.startsWith").unwrapped.asScala.toSeq.map(_.asInstanceOf[String])
        } catch {
          case ex:Exception => Seq[String]()
        },
        endsWith = try {
          config.getList("filters.object.blacklist.endsWith").unwrapped.asScala.toSeq.map(_.asInstanceOf[String])
        } catch {
          case ex:Exception => Seq[String]()
        },
        contains = try {
          config.getList("filters.object.blacklist.contains").unwrapped.asScala.toSeq.map(_.asInstanceOf[String])
        } catch {
          case ex:Exception => Seq[String]()
        },
        equals = try {
          config.getList("filters.object.blacklist.equals").unwrapped.asScala.toSeq.map(_.asInstanceOf[String])
        } catch {
          case ex:Exception => Seq[String]()
        }
      )
    )
  )

}
