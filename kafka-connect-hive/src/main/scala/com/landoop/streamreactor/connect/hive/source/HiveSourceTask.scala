package com.landoop.streamreactor.connect.hive.source

import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util

import com.datamountaineer.streamreactor.connect.utils.JarManifest
import com.landoop.streamreactor.connect.hive.ConfigurationBuilder
import com.landoop.streamreactor.connect.hive.HadoopConfigurationExtension._
import com.landoop.streamreactor.connect.hive.kerberos.KerberosLogin
import com.landoop.streamreactor.connect.hive.sink.config.SinkConfigSettings
import com.landoop.streamreactor.connect.hive.source.config.HiveSourceConfig
import com.landoop.streamreactor.connect.hive.source.offset.{HiveSourceOffsetStorageReader, HiveSourceInitOffsetStorageReader, HiveSourceRefreshOffsetStorageReader}
import com.typesafe.scalalogging.StrictLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.source.{SourceRecord, SourceTask}
import org.apache.parquet.hadoop.ParquetFileReader

import scala.collection.JavaConverters._
import scala.util.Try

class HiveSourceTask extends SourceTask with StrictLogging {

  private val manifest = JarManifest(
    getClass.getProtectionDomain.getCodeSource.getLocation
  )
  private var client: HiveMetaStoreClient = _
  private var fs: FileSystem = _
  private var config: HiveSourceConfig = _

  private var sources: Set[HiveSource] = Set.empty
  private var iterator: Iterator[SourceRecord] = Iterator.empty
  private var kerberosLogin = Option.empty[KerberosLogin]
  private var lastRefresh: Instant = Instant.now()
  private var lastOffsets: Option[Map[SourcePartition, SourceOffset]] = None

  def this(fs: FileSystem, client: HiveMetaStoreClient) {
    this()
    this.client = client
    this.fs = fs
  }

  override def start(props: util.Map[String, String]): Unit = {
    val configs = if (context.configs().isEmpty) props else context.configs()

    config = HiveSourceConfig.fromProps(configs.asScala.toMap)

    Option(fs).foreach(fs => Try(fs.close()))
    Option(client).foreach(c => Try(c.close()))

    val conf: Configuration =
      ConfigurationBuilder.buildHdfsConfiguration(config.hadoopConfiguration)
    conf.set("fs.defaultFS", configs.get(SinkConfigSettings.FsDefaultKey))
    conf.set(ParquetFileReader.PARQUET_READ_PARALLELISM, "1")

    kerberosLogin = config.kerberos.map { kerberos =>
      conf.withKerberos(kerberos)
      KerberosLogin.from(kerberos, conf)
    }

    val hiveConf =
      ConfigurationBuilder.buildHiveConfig(config.hadoopConfiguration)
    hiveConf.set("hive.metastore.local", "false")
    hiveConf.set(
      "hive.metastore",
      configs.get(SinkConfigSettings.MetastoreTypeKey)
    )
    hiveConf.set(
      "hive.metastore.uris",
      configs.get(SinkConfigSettings.MetastoreUrisKey)
    )
    config.kerberos.foreach { _ =>
      val principal =
        Option(configs.get(SinkConfigSettings.HiveMetastorePrincipalKey))
          .getOrElse {
            throw new ConnectException(
              s"Missing configuration for [${SinkConfigSettings.HiveMetastorePrincipalKey}]. When using Kerberos it is required to set the configuration."
            )
          }

      hiveConf.set("hive.metastore.kerberos.principal", principal)
    }

    def initialize(): Unit = {
      fs = FileSystem.get(conf)
      client = new HiveMetaStoreClient(hiveConf)
    }

    kerberosLogin.fold(initialize())(_.run(initialize()))

    val databases = execute(client.getAllDatabases)
    if (!databases.contains(config.dbName.value)) {
      throw new ConnectException(
        s"Cannot find database [${config.dbName.value}]. Current database(-s): ${databases.asScala.mkString(",")}. Please make sure the database is created before sinking to it."
      )
    }

    initSources(contextReader)
  }

  private def contextReader = {
    new HiveSourceInitOffsetStorageReader(context.offsetStorageReader)
  }

  private def initSources(reader: HiveSourceOffsetStorageReader): Unit = {
    lastRefresh = Instant.now()

    sources = config.tableOptions.map { options =>

      new HiveSource(
        config.dbName,
        options.tableName,
        options.topic,
        reader,
        config
      )(client, fs)
    }

    iterator = sources.reduce(
      (a: Iterator[SourceRecord], b: Iterator[SourceRecord]) => a ++ b
    )

  }

  private def originalSourceOffsets = {
    sources.map(_.getOffsets).reduce(_ ++ _)
  }

  override def poll(): util.List[SourceRecord] = {

    refreshIfNecessary()

    iterator.take(config.pollSize).toList.asJava
  }

  private def refreshIfNecessary(): Unit = {
    if (config.refreshFrequency > 0) {
      val nextRefresh = lastRefresh.plus(config.refreshFrequency, ChronoUnit.SECONDS)
      if (Instant.now().isAfter(nextRefresh)) {

        val currentOffsets = originalSourceOffsets
        val allOffsets = lastOffsets
          .fold(currentOffsets) {
            previousOffsets: Map[SourcePartition, SourceOffset] =>
              Map() ++ previousOffsets ++ currentOffsets
          }

        lastOffsets = Some(allOffsets)

        initSources(new HiveSourceRefreshOffsetStorageReader(allOffsets, contextReader))
      }
    }
  }

  override def stop(): Unit = {
    sources.foreach(_.close())
    kerberosLogin.foreach(_.close())
    kerberosLogin = None
  }

  override def version(): String = manifest.version()

  private def execute[T](thunk: => T): T = {
    kerberosLogin match {
      case None => thunk
      case Some(login) => login.run(thunk)
    }
  }
}
