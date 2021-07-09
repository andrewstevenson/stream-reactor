
/*
 * Copyright 2020 Lenses.io
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.lenses.streamreactor.connect.aws.s3.sink

import au.com.bytecode.opencsv.CSVReader
import cats.implicits._
import io.lenses.streamreactor.connect.aws.s3.config.AuthMode
import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings._
import io.lenses.streamreactor.connect.aws.s3.config.processors.ClasspathResourceResolver
import io.lenses.streamreactor.connect.aws.s3.formats.{AvroFormatReader, BytesFormatWriter, ParquetFormatReader}
import io.lenses.streamreactor.connect.aws.s3.sink.utils.S3ProxyContext.{Credential, Identity}
import io.lenses.streamreactor.connect.aws.s3.sink.utils.S3TestPayloadReader._
import io.lenses.streamreactor.connect.aws.s3.sink.utils.{S3ProxyContext, S3TestConfig, S3TestPayloadReader}
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import org.apache.commons.io.IOUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.header.{ConnectHeaders, Header}
import org.apache.kafka.connect.sink.{SinkRecord, SinkTaskContext}
import org.jclouds.blobstore.options.ListContainerOptions
import org.mockito.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.StringReader
import java.{lang, util}
import scala.collection.JavaConverters._

class S3SinkTaskTest extends AnyFlatSpec with Matchers with S3TestConfig with MockitoSugar {

  import io.lenses.streamreactor.connect.aws.s3.sink.utils.TestSampleSchemaAndData._

  private val parquetFormatReader = new ParquetFormatReader()
  private val avroFormatReader = new AvroFormatReader()

  private val PrefixName = "streamReactorBackups"
  private val TopicName = "myTopic"

  private val DeprecatedProps = Map(
    DEP_AWS_ACCESS_KEY -> Identity,
    DEP_AWS_SECRET_KEY -> Credential,
    DEP_AUTH_MODE -> AuthMode.Credentials.toString,
    DEP_CUSTOM_ENDPOINT -> S3ProxyContext.Uri,
    DEP_ENABLE_VIRTUAL_HOST_BUCKETS -> "true"
  )

  private val DefaultProps = Map(
    AWS_ACCESS_KEY -> Identity,
    AWS_SECRET_KEY -> Credential,
    AUTH_MODE -> AuthMode.Credentials.toString,
    CUSTOM_ENDPOINT -> S3ProxyContext.Uri,
    ENABLE_VIRTUAL_HOST_BUCKETS -> "true"
  )

  private val partitionedData: List[Struct] = List(
    new Struct(schema).put("name", "first").put("title", "primary").put("salary", null),
    new Struct(schema).put("name", "second").put("title", "secondary").put("salary", 100.00),
    new Struct(schema).put("name", "third").put("title", "primary").put("salary", 100.00),
    new Struct(schema).put("name", "first").put("title", null).put("salary", 200.00),
    new Struct(schema).put("name", "second").put("title", null).put("salary", 100.00),
    new Struct(schema).put("name", "third").put("title", null).put("salary", 100.00)
  )

  private val headerPartitionedRecords = partitionedData.zipWithIndex.map { case (user, k) =>
    new SinkRecord(TopicName, 1, null, null, schema, user, k, null, null, createHeaders(("headerPartitionKey", (k % 2).toString)))
  }

  private val records = firstUsers.zipWithIndex.map { case (user, k) =>
    new SinkRecord(TopicName, 1, null, null, schema, user, k)
  }

  private val keySchema = SchemaBuilder.struct()
    .field("phonePrefix", SchemaBuilder.string().required().build())
    .field("region", SchemaBuilder.int32().optional().build())
    .build()

  private val keyPartitionedRecords = List(
    new SinkRecord(TopicName, 1, null, createKey(keySchema, ("phonePrefix", "+44"), ("region", 8)), null, users(0), 0, null, null),
    new SinkRecord(TopicName, 1, null, createKey(keySchema, ("phonePrefix", "+49"), ("region", 5)), null, users(1), 1, null, null),
    new SinkRecord(TopicName, 1, null, createKey(keySchema, ("phonePrefix", "+49"), ("region", 5)), null, users(2), 2, null, null)
  )

  "S3SinkTask" should "flush on configured flush time intervals" in {

    val task = new S3SinkTask()

    val props = DeprecatedProps
      .combine(
        Map("connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName WITH_FLUSH_INTERVAL = 1")
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(records.asJava)

    blobStoreContext.getBlobStore.list(BucketName,
      ListContainerOptions.Builder.prefix("streamReactorBackups/myTopic/1")
    ).size() should be(0)

    Thread.sleep(1200) // wait for 1000 millisecond so the next call to put will cause a flush

    task.put(Seq().asJava)
    task.put(records.asJava)

    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    blobStoreContext.getBlobStore.list(BucketName, ListContainerOptions.Builder.prefix("streamReactorBackups/myTopic/1/")).size() should be(1)

    readFileToString(BucketName, "streamReactorBackups/myTopic/1/2.json", blobStoreContext) should be("""{"name":"sam","title":"mr","salary":100.43}{"name":"laura","title":"ms","salary":429.06}{"name":"tom","title":null,"salary":395.44}""")

  }

  "S3SinkTask" should "throw error if prefix contains a slash" in {

    val task = new S3SinkTask()

    val prefixWithSlashes = "my/prefix/that/is/a/path"
    val props = DefaultProps
      .combine(
        Map("connect.s3.kcql" -> s"insert into $BucketName:$prefixWithSlashes select * from $TopicName WITH_FLUSH_INTERVAL = 1")
      ).asJava

    val intercepted = intercept[IllegalArgumentException] {
      task.start(props)
    }

    intercepted.getMessage should be("Nested prefix not currently supported")

  }

  "S3SinkTask" should "flush for every record when configured flush count size of 1" in {

    val task = new S3SinkTask()

    val props = DefaultProps
      .combine(
        Map("connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName WITH_FLUSH_COUNT = 1")
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(records.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    blobStoreContext.getBlobStore.list(BucketName, ListContainerOptions.Builder.prefix("streamReactorBackups/myTopic/1/")).size() should be(3)

    readFileToString(BucketName, "streamReactorBackups/myTopic/1/0.json", blobStoreContext) should be("""{"name":"sam","title":"mr","salary":100.43}""")
    readFileToString(BucketName, "streamReactorBackups/myTopic/1/1.json", blobStoreContext) should be("""{"name":"laura","title":"ms","salary":429.06}""")
    readFileToString(BucketName, "streamReactorBackups/myTopic/1/2.json", blobStoreContext) should be("""{"name":"tom","title":null,"salary":395.44}""")

  }

  /**
    * The file sizes of the 3 records above come out as 44,46,44.
    * We're going to set the threshold to 80 - so once we've written 2 records to a file then the
    * second file should only contain a single record.  This second file won't have been written yet.
    */
  "S3SinkTask" should "flush on configured file size" in {

    val task = new S3SinkTask()

    val props = DefaultProps
      .combine(
        Map(
          "connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName WITH_FLUSH_SIZE = 80"
        )
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(records.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    blobStoreContext.getBlobStore.list(BucketName, ListContainerOptions.Builder.prefix("streamReactorBackups/myTopic/1/")).size() should be(1)

    readFileToString(BucketName, "streamReactorBackups/myTopic/1/1.json", blobStoreContext) should be("""{"name":"sam","title":"mr","salary":100.43}{"name":"laura","title":"ms","salary":429.06}""")

  }

  /**
    * The difference in this test is that the sink is opened again, which will cause the offsets to be copied to the
    * context
    */
  "S3SinkTask" should "put existing offsets to the context" in {

    val task = new S3SinkTask()

    val props = DefaultProps
      .combine(
        Map(
          "connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName WITH_FLUSH_COUNT = 1"
        )
      ).asJava

    val sinkTaskContext = mock[SinkTaskContext]
    task.initialize(sinkTaskContext)
    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(records.asJava)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    blobStoreContext.getBlobStore.list(BucketName, ListContainerOptions.Builder.prefix("streamReactorBackups/myTopic/1/")).size() should be(3)

    readFileToString(BucketName, "streamReactorBackups/myTopic/1/0.json", blobStoreContext) should be("""{"name":"sam","title":"mr","salary":100.43}""")
    readFileToString(BucketName, "streamReactorBackups/myTopic/1/1.json", blobStoreContext) should be("""{"name":"laura","title":"ms","salary":429.06}""")
    readFileToString(BucketName, "streamReactorBackups/myTopic/1/2.json", blobStoreContext) should be("""{"name":"tom","title":null,"salary":395.44}""")

    verify(sinkTaskContext).offset(new TopicPartition("myTopic", 1), 2)
  }

  "S3SinkTask" should "write to parquet format" in {

    val task = new S3SinkTask()

    val props = DefaultProps
      .combine(
        Map("connect.s3.kcql" -> s"""insert into $BucketName:$PrefixName select * from $TopicName STOREAS `PARQUET` WITH_FLUSH_COUNT = 1""")
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(records.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    blobStoreContext.getBlobStore.list(BucketName, ListContainerOptions.Builder.prefix("streamReactorBackups/myTopic/1/")).size() should be(3)

    val bytes = S3TestPayloadReader.readPayload(BucketName, "streamReactorBackups/myTopic/1/0.parquet", blobStoreContext)

    val genericRecords = parquetFormatReader.read(bytes)
    genericRecords.size should be(1)
    checkRecord(genericRecords.head, "sam", "mr", 100.43)

  }

  "S3SinkTask" should "write to avro format" in {

    val task = new S3SinkTask()

    val props = DefaultProps
      .combine(
        Map("connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `AVRO` WITH_FLUSH_COUNT = 1")
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(records.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    blobStoreContext.getBlobStore.list(BucketName, ListContainerOptions.Builder.prefix("streamReactorBackups/myTopic/1/")).size() should be(3)

    val bytes = S3TestPayloadReader.readPayload(BucketName, "streamReactorBackups/myTopic/1/0.avro", blobStoreContext)

    val genericRecords = avroFormatReader.read(bytes)
    genericRecords.size should be(1)
    checkRecord(genericRecords.head, "sam", "mr", 100.43)

  }


  "S3SinkTask" should "error when trying to write AVRO to text format" in {

    val task = new S3SinkTask()

    val props = DefaultProps
      .combine(
        Map("connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `TEXT` WITH_FLUSH_COUNT = 2")
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)

    assertThrows[ConnectException] {
      task.put(records.asJava)
    }

    task.stop()
  }

  "S3SinkTask" should "write to text format" in {

    val textRecords = List(
      new SinkRecord(TopicName, 1, null, null, null, "Sausages", 0),
      new SinkRecord(TopicName, 1, null, null, null, "Mash", 1),
      new SinkRecord(TopicName, 1, null, null, null, "Peas", 2),
      new SinkRecord(TopicName, 1, null, null, null, "Gravy", 3)
    )
    val task = new S3SinkTask()

    val props = DefaultProps
      .combine(
        Map("connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `TEXT` WITH_FLUSH_COUNT = 2")
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(textRecords.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    blobStoreContext.getBlobStore.list(BucketName, ListContainerOptions.Builder.prefix("streamReactorBackups/myTopic/1/")).size() should be(2)

    val file1Bytes = S3TestPayloadReader.readPayload(BucketName, "streamReactorBackups/myTopic/1/1.text", blobStoreContext)
    new String(file1Bytes) should be("Sausages\nMash\n")

    val file2Bytes = S3TestPayloadReader.readPayload(BucketName, "streamReactorBackups/myTopic/1/3.text", blobStoreContext)
    new String(file2Bytes) should be("Peas\nGravy\n")

  }

  "S3SinkTask" should "write to csv format with headers" in {

    val task = new S3SinkTask()

    val extraRecord = new SinkRecord(TopicName, 1, null, null, schema,
      new Struct(schema).put("name", "bob").put("title", "mr").put("salary", 200.86), 3)

    val allRecords: List[SinkRecord] = records :+ extraRecord

    val props = DefaultProps
      .combine(
        Map("connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `CSV_WITHHEADERS` WITH_FLUSH_COUNT = 2")
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(allRecords.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    blobStoreContext.getBlobStore.list(BucketName, ListContainerOptions.Builder.prefix("streamReactorBackups/myTopic/1/")).size() should be(2)

    val file1Bytes = S3TestPayloadReader.readPayload(BucketName, "streamReactorBackups/myTopic/1/1.csv", blobStoreContext)

    val file1Reader = new StringReader(new String(file1Bytes))
    val file1CsvReader = new CSVReader(file1Reader)

    file1CsvReader.readNext() should be(Array("name", "title", "salary"))
    file1CsvReader.readNext() should be(Array("sam", "mr", "100.43"))
    file1CsvReader.readNext() should be(Array("laura", "ms", "429.06"))
    file1CsvReader.readNext() should be(null)

    val file2Bytes = S3TestPayloadReader.readPayload(BucketName, "streamReactorBackups/myTopic/1/3.csv", blobStoreContext)

    val file2Reader = new StringReader(new String(file2Bytes))
    val file2CsvReader = new CSVReader(file2Reader)

    file2CsvReader.readNext() should be(Array("name", "title", "salary"))
    file2CsvReader.readNext() should be(Array("tom", "", "395.44"))
    file2CsvReader.readNext() should be(Array("bob", "mr", "200.86"))
    file2CsvReader.readNext() should be(null)
  }

  "S3SinkTask" should "write to csv format without headers" in {

    val task = new S3SinkTask()

    val extraRecord = new SinkRecord(TopicName, 1, null, null, schema,
      new Struct(schema).put("name", "bob").put("title", "mr").put("salary", 200.86), 3)

    val allRecords: List[SinkRecord] = records :+ extraRecord

    val props = DefaultProps
      .combine(
        Map("connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `CSV` WITH_FLUSH_COUNT = 2")
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(allRecords.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    blobStoreContext.getBlobStore.list(BucketName, ListContainerOptions.Builder.prefix("streamReactorBackups/myTopic/1/")).size() should be(2)

    val file1Bytes = S3TestPayloadReader.readPayload(BucketName, "streamReactorBackups/myTopic/1/1.csv", blobStoreContext)

    val file1Reader = new StringReader(new String(file1Bytes))
    val file1CsvReader = new CSVReader(file1Reader)

    file1CsvReader.readNext() should be(Array("sam", "mr", "100.43"))
    file1CsvReader.readNext() should be(Array("laura", "ms", "429.06"))
    file1CsvReader.readNext() should be(null)

    val file2Bytes = S3TestPayloadReader.readPayload(BucketName, "streamReactorBackups/myTopic/1/3.csv", blobStoreContext)

    val file2Reader = new StringReader(new String(file2Bytes))
    val file2CsvReader = new CSVReader(file2Reader)

    file2CsvReader.readNext() should be(Array("tom", "", "395.44"))
    file2CsvReader.readNext() should be(Array("bob", "mr", "200.86"))
    file2CsvReader.readNext() should be(null)
  }

  "S3SinkTask" should "use custom partitioning scheme and flush for every record" in {

    val task = new S3SinkTask()

    val props = DefaultProps
      .combine(
        Map("connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY name,title,salary WITH_FLUSH_COUNT = 1")
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(headerPartitionedRecords.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    blobStoreContext.getBlobStore.list(BucketName, ListContainerOptions.Builder.recursive().prefix("streamReactorBackups/")).size() should be(6)

    readFileToString(BucketName, "streamReactorBackups/name=first/title=primary/salary=[missing]/myTopic(1_0).json", blobStoreContext) should be("""{"name":"first","title":"primary","salary":null}""")
    readFileToString(BucketName, "streamReactorBackups/name=second/title=secondary/salary=100.0/myTopic(1_1).json", blobStoreContext) should be("""{"name":"second","title":"secondary","salary":100.0}""")
    readFileToString(BucketName, "streamReactorBackups/name=third/title=primary/salary=100.0/myTopic(1_2).json", blobStoreContext) should be("""{"name":"third","title":"primary","salary":100.0}""")
    readFileToString(BucketName, "streamReactorBackups/name=first/title=[missing]/salary=200.0/myTopic(1_3).json", blobStoreContext) should be("""{"name":"first","title":null,"salary":200.0}""")
    readFileToString(BucketName, "streamReactorBackups/name=second/title=[missing]/salary=100.0/myTopic(1_4).json", blobStoreContext) should be("""{"name":"second","title":null,"salary":100.0}""")
    readFileToString(BucketName, "streamReactorBackups/name=third/title=[missing]/salary=100.0/myTopic(1_5).json", blobStoreContext) should be("""{"name":"third","title":null,"salary":100.0}""")

  }

  /**
    * As soon as one file is eligible for writing, it will write all those from the same topic partition.  Therefore 4
    * files are written instead of 2, as there are 2 points at which the write is triggered and the half-full files must
    * be written as well as those reaching the threshold.
    */
  "S3SinkTask" should "use custom partitioning scheme and flush after two written records" in {

    val task = new S3SinkTask()

    val partitionedData: List[Struct] = List(
      new Struct(schema).put("name", "first").put("title", "primary").put("salary", null),
      new Struct(schema).put("name", "first").put("title", "secondary").put("salary", 100.00),
      new Struct(schema).put("name", "first").put("title", "primary").put("salary", 100.00),
      new Struct(schema).put("name", "second").put("title", "secondary").put("salary", 200.00),
      new Struct(schema).put("name", "second").put("title", "primary").put("salary", 100.00),
      new Struct(schema).put("name", "second").put("title", "secondary").put("salary", 100.00)
    )

    val records = partitionedData.zipWithIndex.map { case (user, k) =>
      new SinkRecord(TopicName, 1, null, null, schema, user, k)
    }

    val props = DefaultProps
      .combine(
        Map("connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY name,title WITH_FLUSH_COUNT = 2")
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(records.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    blobStoreContext.getBlobStore.list(BucketName, ListContainerOptions.Builder.recursive().prefix("streamReactorBackups/")).size() should be(4)

    readFileToString(BucketName, "streamReactorBackups/name=first/title=primary/myTopic(1_2).json", blobStoreContext) should be("""{"name":"first","title":"primary","salary":null}{"name":"first","title":"primary","salary":100.0}""")
    readFileToString(BucketName, "streamReactorBackups/name=first/title=secondary/myTopic(1_1).json", blobStoreContext) should be("""{"name":"first","title":"secondary","salary":100.0}""")
    readFileToString(BucketName, "streamReactorBackups/name=second/title=secondary/myTopic(1_5).json", blobStoreContext) should be("""{"name":"second","title":"secondary","salary":200.0}{"name":"second","title":"secondary","salary":100.0}""")
    readFileToString(BucketName, "streamReactorBackups/name=second/title=primary/myTopic(1_4).json", blobStoreContext) should be("""{"name":"second","title":"primary","salary":100.0}""")

  }

  "S3SinkTask" should "use custom partitioning with value display only" in {

    val task = new S3SinkTask()

    val props = DefaultProps
      .combine(
        Map("connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY name,title,salary WITHPARTITIONER=Values WITH_FLUSH_COUNT = 1")
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(headerPartitionedRecords.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    blobStoreContext.getBlobStore.list(BucketName, ListContainerOptions.Builder.recursive().prefix("streamReactorBackups/")).size() should be(6)

    readFileToString(BucketName, "streamReactorBackups/first/primary/[missing]/myTopic(1_0).json", blobStoreContext) should be("""{"name":"first","title":"primary","salary":null}""")
    readFileToString(BucketName, "streamReactorBackups/second/secondary/100.0/myTopic(1_1).json", blobStoreContext) should be("""{"name":"second","title":"secondary","salary":100.0}""")
    readFileToString(BucketName, "streamReactorBackups/third/primary/100.0/myTopic(1_2).json", blobStoreContext) should be("""{"name":"third","title":"primary","salary":100.0}""")
    readFileToString(BucketName, "streamReactorBackups/first/[missing]/200.0/myTopic(1_3).json", blobStoreContext) should be("""{"name":"first","title":null,"salary":200.0}""")
    readFileToString(BucketName, "streamReactorBackups/second/[missing]/100.0/myTopic(1_4).json", blobStoreContext) should be("""{"name":"second","title":null,"salary":100.0}""")
    readFileToString(BucketName, "streamReactorBackups/third/[missing]/100.0/myTopic(1_5).json", blobStoreContext) should be("""{"name":"third","title":null,"salary":100.0}""")

  }

  "S3SinkTask" should "write to bytes format and combine files" in {

    val stream = classOf[BytesFormatWriter].getResourceAsStream("/streamreactor-logo.png")
    val bytes: Array[Byte] = IOUtils.toByteArray(stream)
    val (bytes1, bytes2) = bytes.splitAt(bytes.length / 2)

    val textRecords = List(
      new SinkRecord(TopicName, 1, null, null, null, bytes1, 0),
      new SinkRecord(TopicName, 1, null, null, null, bytes2, 1)
    )
    val task = new S3SinkTask()

    val props = DefaultProps
      .combine(
        Map("connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `BYTES_ValueOnly` WITH_FLUSH_COUNT = 2")
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(textRecords.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    blobStoreContext.getBlobStore.list(BucketName, ListContainerOptions.Builder.prefix("streamReactorBackups/myTopic/1/")).size() should be(1)

    val file1Bytes = S3TestPayloadReader.readPayload(BucketName, "streamReactorBackups/myTopic/1/1.bytes", blobStoreContext)
    file1Bytes should be(bytes)

  }


  "S3SinkTask" should "support header values for data partitioning" in {

    val textRecords = List(
      new SinkRecord(TopicName, 1, null, null, null, users(0), 0, null, null, createHeaders(("phonePrefix", "+44"), ("region", "8"))),
      new SinkRecord(TopicName, 1, null, null, null, users(1), 1, null, null, createHeaders(("phonePrefix", "+49"), ("region", "5"))),
      new SinkRecord(TopicName, 1, null, null, null, users(2), 2, null, null, createHeaders(("phonePrefix", "+49"), ("region", "5")))
    )
    val task = new S3SinkTask()

    val props = DefaultProps
      .combine(
        Map("connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY _header.phonePrefix,_header.region STOREAS `CSV` WITH_FLUSH_COUNT = 1")
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(textRecords.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    blobStoreContext.getBlobStore.list(BucketName, ListContainerOptions.Builder.prefix("streamReactorBackups/").recursive()).size() should be(3)

    val file1CsvReader: CSVReader = openCsvReaderToBucketFile("streamReactorBackups/phonePrefix=+44/region=8/myTopic(1_0).csv")
    file1CsvReader.readNext() should be(Array("sam", "mr", "100.43"))
    file1CsvReader.readNext() should be(null)

    val file2CsvReader: CSVReader = openCsvReaderToBucketFile("streamReactorBackups/phonePrefix=+49/region=5/myTopic(1_1).csv")
    file2CsvReader.readNext() should be(Array("laura", "ms", "429.06"))
    file2CsvReader.readNext() should be(null)

    val file3CsvReader: CSVReader = openCsvReaderToBucketFile("streamReactorBackups/phonePrefix=+49/region=5/myTopic(1_2).csv")
    file3CsvReader.readNext() should be(Array("tom", "", "395.44"))
    file3CsvReader.readNext() should be(null)
  }

  "S3SinkTask" should "combine header and value-extracted partition" in {

    val task = new S3SinkTask()

    val props = DefaultProps
      .combine(
        Map("connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY _header.headerPartitionKey,_value.name STOREAS `CSV` WITH_FLUSH_COUNT = 1")
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(headerPartitionedRecords.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    val fileList = blobStoreContext.getBlobStore.list(BucketName, ListContainerOptions.Builder.prefix("streamReactorBackups/").recursive())
    fileList.size() should be(6)

    fileList.asScala.map(file => file.getName) should contain allOf(
      "streamReactorBackups/headerPartitionKey=0/name=first/myTopic(1_0).csv",
      "streamReactorBackups/headerPartitionKey=1/name=second/myTopic(1_1).csv",
      "streamReactorBackups/headerPartitionKey=0/name=third/myTopic(1_2).csv",
      "streamReactorBackups/headerPartitionKey=1/name=first/myTopic(1_3).csv",
      "streamReactorBackups/headerPartitionKey=0/name=second/myTopic(1_4).csv",
      "streamReactorBackups/headerPartitionKey=1/name=third/myTopic(1_5).csv"
    )
  }

  "S3SinkTask" should "fail when header value is missing from the record when header partitioning activated" in {

    val textRecords = List(
      createSinkRecord(1, users(0), 0, createHeaders(("feet", "5"))),
      createSinkRecord(1, users(1), 0, createHeaders(("hair", "blue"), ("feet", "5"))),
    )

    val task = new S3SinkTask()

    val props = DefaultProps
      .combine(
        Map("connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY _header.hair,_header.feet STOREAS `CSV` WITH_FLUSH_COUNT = 1")
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    val caught = intercept[ConnectException] {
      task.put(textRecords.asJava)
    }

    assert(caught.getMessage.endsWith("Header 'hair' not found in message"))

    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    blobStoreContext.getBlobStore.list(BucketName, ListContainerOptions.Builder.prefix("streamReactorBackups/").recursive()).size() should be(0)

  }

  "S3SinkTask" should "support numeric header data types" in {

    val textRecords = List(
      createSinkRecord(1, users(0), 0, createHeaders(("intheader", 1), ("longheader", 2L))),
      createSinkRecord(1, users(1), 1, createHeaders(("longheader", 2L), ("intheader", 2))),
      createSinkRecord(2, users(2), 2, createHeaders(("intheader", 1), ("longheader", 1L))),
    )
    val task = new S3SinkTask()

    val props = DefaultProps
      .combine(
        Map("connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY _header.intheader,_header.longheader STOREAS `CSV` WITH_FLUSH_COUNT = 1")
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(textRecords.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    val fileList = blobStoreContext.getBlobStore.list(BucketName, ListContainerOptions.Builder.prefix("streamReactorBackups/").recursive())
    fileList.size() should be(3)

    fileList.asScala.map(file => file.getName) should contain allOf(
      "streamReactorBackups/intheader=1/longheader=2/myTopic(1_0).csv",
      "streamReactorBackups/intheader=2/longheader=2/myTopic(1_1).csv",
      "streamReactorBackups/intheader=1/longheader=1/myTopic(2_2).csv",
    )
  }

  "S3SinkTask" should "partition by whole of kafka message key with key label" in {

    val keyPartitionedRecords = partitionedData.zipWithIndex.map { case (user, k) =>
      new SinkRecord(TopicName, 1, null, (k % 2).toString, schema, user, k, null, null)
    }

    val task = new S3SinkTask()

    val props = DefaultProps
      .combine(
        Map("connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY _key STOREAS `CSV` WITH_FLUSH_COUNT = 1")
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(keyPartitionedRecords.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    val fileList = blobStoreContext.getBlobStore.list(BucketName, ListContainerOptions.Builder.prefix("streamReactorBackups/").recursive())
    fileList.size() should be(6)

    fileList.asScala.map(file => file.getName) should contain allOf(
      "streamReactorBackups/key=0/myTopic(1_0).csv",
      "streamReactorBackups/key=1/myTopic(1_1).csv",
      "streamReactorBackups/key=0/myTopic(1_2).csv",
      "streamReactorBackups/key=1/myTopic(1_3).csv",
      "streamReactorBackups/key=0/myTopic(1_4).csv",
      "streamReactorBackups/key=1/myTopic(1_5).csv"
    )
  }

  "S3SinkTask" should "partition by whole of kafka message key without key label" in {

    val keyPartitionedRecords = partitionedData.zipWithIndex.map { case (user, k) =>
      new SinkRecord(TopicName, 1, null, (k % 2).toString, schema, user, k, null, null)
    }

    val task = new S3SinkTask()

    val props = DefaultProps
      .combine(
        Map("connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY _key STOREAS `CSV` WITHPARTITIONER=Values WITH_FLUSH_COUNT = 1")
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(keyPartitionedRecords.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    val fileList = blobStoreContext.getBlobStore.list(BucketName, ListContainerOptions.Builder.prefix("streamReactorBackups/").recursive())
    fileList.size() should be(6)

    fileList.asScala.map(file => file.getName) should contain allOf(
      "streamReactorBackups/0/myTopic(1_0).csv",
      "streamReactorBackups/1/myTopic(1_1).csv",
      "streamReactorBackups/0/myTopic(1_2).csv",
      "streamReactorBackups/1/myTopic(1_3).csv",
      "streamReactorBackups/0/myTopic(1_4).csv",
      "streamReactorBackups/1/myTopic(1_5).csv"
    )
  }

  "S3SinkTask" should "partition by custom partitioning with topic/partition/offset" in {

    val keyPartitionedRecords = partitionedData.zipWithIndex.map { case (user, k) =>
      new SinkRecord(TopicName, 1, null, (k % 2).toString, schema, user, k, null, null)
    }

    val task = new S3SinkTask()

    val props = DefaultProps
      .combine(
        Map("connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY _topic, _partition STOREAS `CSV` WITHPARTITIONER=Values WITH_FLUSH_COUNT = 1")
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(keyPartitionedRecords.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    val fileList = blobStoreContext.getBlobStore.list(BucketName, ListContainerOptions.Builder.prefix("streamReactorBackups/").recursive())
    fileList.size() should be(6)

    fileList.asScala.map(file => file.getName) should contain allOf(
      "streamReactorBackups/myTopic/1/myTopic(1_0).csv",
      "streamReactorBackups/myTopic/1/myTopic(1_1).csv",
      "streamReactorBackups/myTopic/1/myTopic(1_2).csv",
      "streamReactorBackups/myTopic/1/myTopic(1_3).csv",
      "streamReactorBackups/myTopic/1/myTopic(1_4).csv",
      "streamReactorBackups/myTopic/1/myTopic(1_5).csv"
    )
  }

  "S3SinkTask" should "fail if the key is non-primitive but requests _key partitioning" in {

    val keyPartitionedRecords = partitionedData.zipWithIndex.map { case (user, k) =>
      new SinkRecord(TopicName, 1, null, user, schema, user, k, null, null)
    }

    val task = new S3SinkTask()

    val props = DefaultProps
      .combine(
        Map("connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY _key STOREAS `CSV` WITH_FLUSH_COUNT = 1")
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    val intercepted = intercept[ConnectException] {
      task.put(keyPartitionedRecords.asJava)
    }
    intercepted.getMessage should endWith("Non primitive struct provided, PARTITIONBY _key requested in KCQL")

    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    val fileList = blobStoreContext.getBlobStore.list(BucketName, ListContainerOptions.Builder.prefix("streamReactorBackups/").recursive())
    fileList.size() should be(0)

  }

  "S3SinkTask" should "allow a primitive int key for _key partitioning" in {

    val keyPartitionedRecords = partitionedData.zipWithIndex.map { case (user, k) =>
      new SinkRecord(TopicName, 1, null, k % 2, schema, user, k, null, null)
    }

    val task = new S3SinkTask()

    val props = DefaultProps
      .combine(
        Map("connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY _key STOREAS `CSV` WITHPARTITIONER=Values WITH_FLUSH_COUNT = 1")
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(keyPartitionedRecords.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    val fileList = blobStoreContext.getBlobStore.list(BucketName, ListContainerOptions.Builder.prefix("streamReactorBackups/").recursive())
    fileList.size() should be(6)

    fileList.asScala.map(file => file.getName) should contain allOf(
      "streamReactorBackups/0/myTopic(1_0).csv",
      "streamReactorBackups/1/myTopic(1_1).csv",
      "streamReactorBackups/0/myTopic(1_2).csv",
      "streamReactorBackups/1/myTopic(1_3).csv",
      "streamReactorBackups/0/myTopic(1_4).csv",
      "streamReactorBackups/1/myTopic(1_5).csv"
    )
  }

  "S3SinkTask" should "allow partitioning by complex key" in {

    val task = new S3SinkTask()

    val props = DefaultProps
      .combine(
        Map("connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY _key.region, _key.phonePrefix STOREAS `CSV` WITH_FLUSH_COUNT = 1")
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(keyPartitionedRecords.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    val fileList = blobStoreContext.getBlobStore.list(BucketName, ListContainerOptions.Builder.prefix("streamReactorBackups/").recursive())
    fileList.size() should be(3)

    fileList.asScala.map(file => file.getName) should contain allOf(
      "streamReactorBackups/region=8/phonePrefix=+44/myTopic(1_0).csv",
      "streamReactorBackups/region=5/phonePrefix=+49/myTopic(1_1).csv",
      "streamReactorBackups/region=5/phonePrefix=+49/myTopic(1_2).csv"
    )
  }

  "S3SinkTask" should "not get passed csql parser when contains a slash" in {

    val task = new S3SinkTask()

    val keyWithSlash = "_key.date/of/birth"

    val props = DefaultProps
      .combine(
        Map("connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY $keyWithSlash, _key.phonePrefix STOREAS `CSV` WITH_FLUSH_COUNT = 1")
      ).asJava

    intercept[IllegalArgumentException] {
      task.start(props)
    }.getMessage contains "no viable alternative at input"

  }

  "S3SinkTask" should "allow partitioning by complex key and values" in {

    val task = new S3SinkTask()

    val props = DefaultProps
      .combine(
        Map("connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY _key.region, name WITH_FLUSH_COUNT = 1")
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(keyPartitionedRecords.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    val fileList = blobStoreContext.getBlobStore.list(BucketName, ListContainerOptions.Builder.prefix("streamReactorBackups/").recursive())
    fileList.size() should be(3)

    fileList.asScala.map(file => file.getName) should contain allOf(
      "streamReactorBackups/region=8/name=sam/myTopic(1_0).json",
      "streamReactorBackups/region=5/name=laura/myTopic(1_1).json",
      "streamReactorBackups/region=5/name=tom/myTopic(1_2).json"
    )
  }

  /**
    * This should write partition 1 but not partition 0
    */
  "S3SinkTask" should "write multiple partitions independently" in {

    val kafkaPartitionedRecords = List(
      new SinkRecord(TopicName, 0, null, null, schema, users(0), 0),
      new SinkRecord(TopicName, 1, null, null, schema, users(1), 0),
      new SinkRecord(TopicName, 1, null, null, schema, users(2), 1)
    )

    val topicPartitionsToManage = Seq(
      new TopicPartition(TopicName, 0),
      new TopicPartition(TopicName, 1)
    ).asJava

    val task = new S3SinkTask()

    val props = DefaultProps
      .combine(
        Map("connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName WITH_FLUSH_COUNT = 2")
      ).asJava

    task.start(props)

    task.open(topicPartitionsToManage)
    task.put(kafkaPartitionedRecords.asJava)
    task.close(topicPartitionsToManage)
    task.stop()

    val fileList = blobStoreContext.getBlobStore.list(BucketName, ListContainerOptions.Builder.prefix("streamReactorBackups/").recursive())
    fileList.size() should be(1)

    fileList.asScala.map(file => file.getName) should contain(
      "streamReactorBackups/myTopic/1/1.json",
    )
  }

  "S3SinkTask" should "process and partition records by a map value" in {

    val map = Map(
      "jedi" -> 1,
      "klingons" -> 2,
      "cylons" -> 3
    ).asJava

    val kafkaPartitionedRecords = List(
      new SinkRecord(TopicName, 0, null, null, SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build(), map, 0)
    )

    val topicPartitionsToManage = Seq(
      new TopicPartition(TopicName, 0)
    ).asJava

    val task = new S3SinkTask()

    val props = DefaultProps
      .combine(
        Map("connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY jedi WITH_FLUSH_COUNT = 1")
      ).asJava

    task.start(props)

    task.open(topicPartitionsToManage)
    task.put(kafkaPartitionedRecords.asJava)
    task.close(topicPartitionsToManage)
    task.stop()

    val fileList = blobStoreContext.getBlobStore.list(BucketName, ListContainerOptions.Builder.prefix("streamReactorBackups/").recursive())
    fileList.size() should be(1)

    fileList.asScala.map(file => file.getName) should contain(
      "streamReactorBackups/jedi=1/myTopic(0_0).json",
    )
  }

  "S3SinkTask" should "process and partition records by an array value json" in {

    val array = Array(
      "jedi",
      "klingons",
      "cylons"
    )

    val kafkaPartitionedRecords = List(
      new SinkRecord(TopicName, 0, null, null, null, array, 0)
    )

    val topicPartitionsToManage = Seq(
      new TopicPartition(TopicName, 0)
    ).asJava

    val task = new S3SinkTask()

    val props = DefaultProps
      .combine(
        Map("connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName WITH_FLUSH_COUNT = 1")
      ).asJava

    task.start(props)

    task.open(topicPartitionsToManage)
    task.put(kafkaPartitionedRecords.asJava)
    task.close(topicPartitionsToManage)
    task.stop()

    val fileList = blobStoreContext.getBlobStore.list(BucketName, ListContainerOptions.Builder.prefix("streamReactorBackups/").recursive())
    fileList.size() should be(1)

    readFileToString(BucketName, "streamReactorBackups/myTopic/0/0.json", blobStoreContext) should be("""["jedi","klingons","cylons"]""")

  }

  "S3SinkTask" should "process and partition records by an array value avro" in {

    val array = List(
      "jedi",
      "klingons",
      "cylons"
    ).asJava

    val kafkaPartitionedRecords = List(
      new SinkRecord(TopicName, 0, null, null, SchemaBuilder.array(Schema.STRING_SCHEMA), array, 0)
    )

    val topicPartitionsToManage = Seq(
      new TopicPartition(TopicName, 0)
    ).asJava

    val task = new S3SinkTask()

    val props = DefaultProps
      .combine(
        Map("connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `AVRO` WITH_FLUSH_COUNT = 1")
      ).asJava

    task.start(props)

    task.open(topicPartitionsToManage)
    task.put(kafkaPartitionedRecords.asJava)
    task.close(topicPartitionsToManage)
    task.stop()

    blobStoreContext.getBlobStore.list(BucketName, ListContainerOptions.Builder.prefix("streamReactorBackups/myTopic/0/")).size() should be(1)

    val bytes = S3TestPayloadReader.readPayload(BucketName, "streamReactorBackups/myTopic/0/0.avro", blobStoreContext)

    val genericRecords = avroFormatReader.read(bytes)
    genericRecords.size should be(1)
    checkArray(genericRecords.head.asInstanceOf[GenericData.Array[Utf8]], "jedi", "klingons", "cylons")
  }

  "S3SinkTask" should "process a map of structs avro" in {

    val map = Map(
      "jedi" -> users(0),
      "klingons" -> users(1),
      "cylons" -> users(2)
    ).asJava

    val kafkaPartitionedRecords = List(
      new SinkRecord(TopicName, 0, null, null, SchemaBuilder.map(Schema.STRING_SCHEMA, schema), map, 0)
    )

    val topicPartitionsToManage = Seq(
      new TopicPartition(TopicName, 0)
    ).asJava

    val task = new S3SinkTask()

    val props = DefaultProps
      .combine(
        Map("connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `AVRO` WITH_FLUSH_COUNT = 1")
      ).asJava

    task.start(props)

    task.open(topicPartitionsToManage)
    task.put(kafkaPartitionedRecords.asJava)
    task.close(topicPartitionsToManage)
    task.stop()

    blobStoreContext.getBlobStore.list(BucketName, ListContainerOptions.Builder.prefix("streamReactorBackups/myTopic/0/")).size() should be(1)

    val bytes = S3TestPayloadReader.readPayload(BucketName, "streamReactorBackups/myTopic/0/0.avro", blobStoreContext)

    val genericRecords = avroFormatReader.read(bytes)
    genericRecords.size should be(1)

    val recordsMap = genericRecords.head.asInstanceOf[util.Map[Utf8, GenericData.Record]].asScala

    checkRecord(recordsMap.getOrElse(new Utf8("jedi"), fail("can't find in map")), "sam", Some("mr"), 100.43)
    checkRecord(recordsMap.getOrElse(new Utf8("klingons"), fail("can't find in map")), "laura", Some("ms"), 429.06)
    checkRecord(recordsMap.getOrElse(new Utf8("cylons"), fail("can't find in map")), "tom", None, 395.44)

  }

  "S3SinkTask" should "process a map of structs with nulls avro" in {

    val map = Map(
      "jedi" -> users(0),
      "cylons" -> null
    ).asJava

    val optionalSchema: Schema = SchemaBuilder.struct()
      .optional()
      .field("name", SchemaBuilder.string().required().build())
      .field("title", SchemaBuilder.string().optional().build())
      .field("salary", SchemaBuilder.float64().optional().build())
      .build()

    val kafkaPartitionedRecords = List(
      new SinkRecord(TopicName, 0, null, null, SchemaBuilder.map(Schema.STRING_SCHEMA, optionalSchema).build(), map, 0)
    )

    val topicPartitionsToManage = Seq(
      new TopicPartition(TopicName, 0)
    ).asJava

    val task = new S3SinkTask()

    val props = DefaultProps
      .combine(
        Map("connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `AVRO` WITH_FLUSH_COUNT = 1")
      ).asJava

    task.start(props)

    task.open(topicPartitionsToManage)
    task.put(kafkaPartitionedRecords.asJava)
    task.close(topicPartitionsToManage)
    task.stop()

    blobStoreContext.getBlobStore.list(BucketName, ListContainerOptions.Builder.prefix("streamReactorBackups/myTopic/0/")).size() should be(1)

    val bytes = S3TestPayloadReader.readPayload(BucketName, "streamReactorBackups/myTopic/0/0.avro", blobStoreContext)

    val genericRecords = avroFormatReader.read(bytes)
    genericRecords.size should be(1)

    val recordsMap = genericRecords.head.asInstanceOf[util.Map[Utf8, GenericData.Record]].asScala

    checkRecord(recordsMap.getOrElse(new Utf8("jedi"), fail("can't find in map")), "sam", Some("mr"), 100.43)
    recordsMap.getOrElse(new Utf8("cylons"), fail("can't find in map")) should be(null)

  }

  "S3SinkTask" should "process a map of structs with nulls json" in {

    val keySchema = Schema.STRING_SCHEMA
    val valSchema = SchemaBuilder.struct()
      .optional()
      .field("name", SchemaBuilder.string().required().build())
      .field("title", SchemaBuilder.string().optional().build())
      .field("salary", SchemaBuilder.float64().optional().build())
      .build()
    val mapSchema = SchemaBuilder.map(
      keySchema,
      valSchema
    ).build()

    val map = Map(
      "jedi" -> new Struct(valSchema).put("name", "sam").put("title", "mr").put("salary", 100.43),
      "cylons" -> null
    ).asJava


    val kafkaPartitionedRecords = List(
      new SinkRecord(TopicName, 0, null, null, mapSchema, map, 0)
    )

    val topicPartitionsToManage = Seq(
      new TopicPartition(TopicName, 0)
    ).asJava

    val task = new S3SinkTask()

    val props = DefaultProps
      .combine(
        Map("connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `JSON` WITH_FLUSH_COUNT = 1")
      ).asJava

    task.start(props)

    task.open(topicPartitionsToManage)
    task.put(kafkaPartitionedRecords.asJava)
    task.close(topicPartitionsToManage)
    task.stop()

    blobStoreContext.getBlobStore.list(BucketName, ListContainerOptions.Builder.prefix("streamReactorBackups/myTopic/0/")).size() should be(1)

    readFileToString(BucketName, "streamReactorBackups/myTopic/0/0.json", blobStoreContext) should be("""{"jedi":{"name":"sam","title":"mr","salary":100.43},"cylons":null}""")

  }

  "S3SinkTask" should "process and map of arrays avro" in {

    val map = Map(
      "jedi" -> Array("bob", "john"),
      "klingons" -> Array("joe"),
      "cylons" -> Array("merv", "marv")
    ).asJava

    val kafkaPartitionedRecords = List(
      new SinkRecord(TopicName, 0, null, null, SchemaBuilder.map(Schema.STRING_SCHEMA, SchemaBuilder.array(Schema.STRING_SCHEMA)), map, 0)
    )

    val topicPartitionsToManage = Seq(
      new TopicPartition(TopicName, 0)
    ).asJava

    val task = new S3SinkTask()

    val props = DefaultProps
      .combine(
        Map("connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `AVRO` WITH_FLUSH_COUNT = 1")
      ).asJava

    task.start(props)

    task.open(topicPartitionsToManage)
    task.put(kafkaPartitionedRecords.asJava)
    task.close(topicPartitionsToManage)
    task.stop()

    blobStoreContext.getBlobStore.list(BucketName, ListContainerOptions.Builder.prefix("streamReactorBackups/myTopic/0/")).size() should be(1)

    val bytes = S3TestPayloadReader.readPayload(BucketName, "streamReactorBackups/myTopic/0/0.avro", blobStoreContext)

    val genericRecords = avroFormatReader.read(bytes)
    genericRecords.size should be(1)

    val recordsMap = genericRecords.head.asInstanceOf[util.Map[Utf8, GenericData.Array[Utf8]]].asScala

    checkArray(recordsMap.getOrElse(new Utf8("jedi"), fail("can't find in map")), "bob", "john")
    checkArray(recordsMap.getOrElse(new Utf8("klingons"), fail("can't find in map")), "joe")
    checkArray(recordsMap.getOrElse(new Utf8("cylons"), fail("can't find in map")), "merv", "marv")

  }

  val addressSchema: Schema = SchemaBuilder.struct()
    .field("number", SchemaBuilder.int32().required().build())
    .field("city", SchemaBuilder.string().optional().build())

  val nestedSchema: Schema = SchemaBuilder.struct()
    .field("user", schema)
    .field("address", addressSchema)
    .build()

  val nested: List[Struct] = List(
    new Struct(nestedSchema)
      .put("user", new Struct(schema)
        .put("name", "sam")
        .put("title", "mr")
        .put("salary", 100.43),
      )
      .put("address", new Struct(addressSchema)
        .put("number", 1)
        .put("city", "bristol")
      ),
    new Struct(nestedSchema)
      .put("user", new Struct(schema)
        .put("name", "laura")
        .put("title", "ms")
        .put("salary", 429.06),
      )
      .put("address", new Struct(addressSchema)
        .put("number", 1)
        .put("city", "bristol")
      ),
    new Struct(nestedSchema)
      .put("user", new Struct(schema)
        .put("name", "tom")
        .put("title", null)
        .put("salary", 395.44),
      )
      .put("address", new Struct(addressSchema)
        .put("number", 1)
        .put("city", "bristol")
      )
  )

  /**
    * This should write partition 1 but not partition 0
    */
  "S3SinkTask" should "partition by nested value fields" in {

    val kafkaPartitionedRecords = List(
      new SinkRecord(TopicName, 0, null, null, schema, nested(0), 0),
      new SinkRecord(TopicName, 1, null, null, schema, nested(1), 0),
      new SinkRecord(TopicName, 1, null, null, schema, nested(2), 1)
    )

    val topicPartitionsToManage = Seq(
      new TopicPartition(TopicName, 0),
      new TopicPartition(TopicName, 1)
    ).asJava

    val task = new S3SinkTask()

    val props = DefaultProps
      .combine(
        Map("connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY _value.user.name WITH_FLUSH_COUNT = 1")
      ).asJava

    task.start(props)

    task.open(topicPartitionsToManage)
    task.put(kafkaPartitionedRecords.asJava)
    task.close(topicPartitionsToManage)
    task.stop()

    val fileList = blobStoreContext.getBlobStore.list(BucketName, ListContainerOptions.Builder.prefix("streamReactorBackups/").recursive())
    fileList.size() should be(3)

    fileList.asScala.map(file => file.getName) should contain allOf(
      "streamReactorBackups/user.name=laura/myTopic(1_0).json",
      "streamReactorBackups/user.name=sam/myTopic(0_0).json",
      "streamReactorBackups/user.name=tom/myTopic(1_1).json",
    )
  }


  /**
    * This should write partition 1 but not partition 0
    */
  "S3SinkTask" should "partition by nested key fields" in {

    val favsSchema: Schema = SchemaBuilder.map(SchemaBuilder.string().build(), SchemaBuilder.string().build())

    val structMapSchema: Schema = SchemaBuilder.struct()
      .field("user", schema)
      .field("favourites", favsSchema)
      .build()

    val nested: List[Struct] = List(
      new Struct(structMapSchema)
        .put("user", new Struct(schema)
          .put("name", "sam")
          .put("title", "mr")
          .put("salary", 100.43),
        )
        .put(
          "favourites",
          Map("band" -> "the killers", "film" -> "a clockwork orange").asJava
        ),
      new Struct(structMapSchema)
        .put("user", new Struct(schema)
          .put("name", "laura")
          .put("title", "ms")
          .put("salary", 429.06),
        )
        .put(
          "favourites",
          Map("band" -> "the strokes", "film" -> "a clockwork orange").asJava
        ),
      new Struct(structMapSchema)
        .put("user", new Struct(schema)
          .put("name", "tom")
          .put("title", null)
          .put("salary", 395.44),
        )
        .put(
          "favourites",
          Map().asJava
        ),
    )

    val kafkaPartitionedRecords = List(
      new SinkRecord(TopicName, 0, structMapSchema, nested(0), SchemaBuilder.int32().build(), 0, 0),
      new SinkRecord(TopicName, 1, structMapSchema, nested(1), SchemaBuilder.int32().build(), 1, 0),
      new SinkRecord(TopicName, 1, structMapSchema, nested(2), SchemaBuilder.int32().build(), 2, 1),
    )

    val topicPartitionsToManage = Seq(
      new TopicPartition(TopicName, 0),
      new TopicPartition(TopicName, 1)
    ).asJava

    val task = new S3SinkTask()

    val props = DefaultProps
      .combine(
        Map("connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY _key.favourites.band WITH_FLUSH_COUNT = 1")
      ).asJava

    task.start(props)

    task.open(topicPartitionsToManage)
    task.put(kafkaPartitionedRecords.asJava)
    task.close(topicPartitionsToManage)
    task.stop()

    val fileList = blobStoreContext.getBlobStore.list(BucketName, ListContainerOptions.Builder.prefix("streamReactorBackups/").recursive())
    fileList.size() should be(3)

    fileList.asScala.map(file => file.getName) should contain allOf(
      "streamReactorBackups/favourites.band=the strokes/myTopic(1_0).json",
      "streamReactorBackups/favourites.band=the killers/myTopic(0_0).json",
      "streamReactorBackups/favourites.band=[missing]/myTopic(1_1).json",
    )
  }


  /**
    * This should write partition 1 but not partition 0
    */
  "S3SinkTask" should "partition by nested header fields" in {

    val kafkaPartitionedRecords = List(
      new SinkRecord(TopicName, 0, null, null, SchemaBuilder.int32().build(), 0, 0, null, null, createHeaders("header1" -> nested(0))),
      new SinkRecord(TopicName, 1, null, null, SchemaBuilder.int32().build(), 1, 0, null, null, createHeaders("header1" -> nested(1))),
      new SinkRecord(TopicName, 1, null, null, SchemaBuilder.int32().build(), 2, 1, null, null, createHeaders("header1" -> nested(2))),
    )

    val topicPartitionsToManage = Seq(
      new TopicPartition(TopicName, 0),
      new TopicPartition(TopicName, 1)
    ).asJava

    val task = new S3SinkTask()

    val props = DefaultProps
      .combine(
        Map("connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName PARTITIONBY _header.header1.user.name WITH_FLUSH_COUNT = 1")
      ).asJava

    task.start(props)

    task.open(topicPartitionsToManage)
    task.put(kafkaPartitionedRecords.asJava)
    task.close(topicPartitionsToManage)
    task.stop()

    val fileList = blobStoreContext.getBlobStore.list(BucketName, ListContainerOptions.Builder.prefix("streamReactorBackups/").recursive())
    fileList.size() should be(3)

    fileList.asScala.map(file => file.getName) should contain allOf(
      "streamReactorBackups/header1.user.name=laura/myTopic(1_0).json",
      "streamReactorBackups/header1.user.name=sam/myTopic(0_0).json",
      "streamReactorBackups/header1.user.name=tom/myTopic(1_1).json",
    )
  }

  "S3SinkTask" should "flush for every record when configured flush count size of 1 with build local write mode" in {

    val task = new S3SinkTask()

    val props = DefaultProps
      .combine(
        Map(
          "name" -> "s3SinkTaskBuildLocalTest",
          "connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName WITH_FLUSH_COUNT = 1",
          "connect.s3.write.mode" -> "BuildLocal",
        )
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(records.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    blobStoreContext.getBlobStore.list(BucketName, ListContainerOptions.Builder.prefix("streamReactorBackups/myTopic/1/")).size() should be(3)

    readFileToString(BucketName, "streamReactorBackups/myTopic/1/0.json", blobStoreContext) should be("""{"name":"sam","title":"mr","salary":100.43}""")
    readFileToString(BucketName, "streamReactorBackups/myTopic/1/1.json", blobStoreContext) should be("""{"name":"laura","title":"ms","salary":429.06}""")
    readFileToString(BucketName, "streamReactorBackups/myTopic/1/2.json", blobStoreContext) should be("""{"name":"tom","title":null,"salary":395.44}""")

  }

  "S3SinkTask" should "read from profile" in {

    val profileDir = ClasspathResourceResolver.getResourcesDirectory() match {
      case Left(ex) => fail("cannot get resources dir", ex)
      case Right(value) => value
    }
    val task = new S3SinkTask()

    val props = Map(
      PROFILES -> s"$profileDir/inttest1.yaml,$profileDir/inttest2.yaml"
    ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.put(records.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    blobStoreContext.getBlobStore.list(BucketName, ListContainerOptions.Builder.prefix("streamReactorBackups/myTopic/1/")).size() should be(3)

    readFileToString(BucketName, "streamReactorBackups/myTopic/1/0.json", blobStoreContext) should be("""{"name":"sam","title":"mr","salary":100.43}""")
    readFileToString(BucketName, "streamReactorBackups/myTopic/1/1.json", blobStoreContext) should be("""{"name":"laura","title":"ms","salary":429.06}""")
    readFileToString(BucketName, "streamReactorBackups/myTopic/1/2.json", blobStoreContext) should be("""{"name":"tom","title":null,"salary":395.44}""")

  }

  private def createSinkRecord(partition: Int, valueStruct: Struct, offset: Int, headers: lang.Iterable[Header]) = {
    new SinkRecord(TopicName, partition, null, null, null, valueStruct, offset, null, null, headers)
  }

  private def createKey(keySchema: Schema, keyValuePair: (String, Any)*): Struct = {
    val struct = new Struct(keySchema)
    keyValuePair.foreach {
      case (key: String, value) => struct.put(key, value)
    }
    struct
  }

  private def createHeaders[T](keyValuePair: (String, T)*): lang.Iterable[Header] = {
    val headers = new ConnectHeaders()
    keyValuePair.foreach {
      case (key: String, value) => headers.add(key, value, null)
    }
    headers
  }

  private def openCsvReaderToBucketFile(bucketFileName: String) = {
    val file1Bytes = S3TestPayloadReader.readPayload(BucketName, bucketFileName, blobStoreContext)

    val file1Reader = new StringReader(new String(file1Bytes))
    new CSVReader(file1Reader)
  }
}
