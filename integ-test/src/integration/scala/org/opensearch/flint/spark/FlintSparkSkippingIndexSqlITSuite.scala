/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import scala.Option.empty
import scala.collection.JavaConverters.{mapAsJavaMapConverter, mapAsScalaMapConverter}

import com.stephenn.scalatest.jsonassert.JsonMatchers.matchJson
import org.json4s.{Formats, NoTypeHints}
import org.json4s.native.JsonMethods.{compact, parse, render}
import org.json4s.native.Serialization
import org.opensearch.client.RequestOptions
import org.opensearch.client.indices.GetIndexRequest
import org.opensearch.flint.core.FlintOptions
import org.opensearch.flint.core.storage.{FlintOpenSearchIndexMetadataService, OpenSearchClientUtils}
import org.opensearch.flint.spark.skipping.FlintSparkSkippingIndex.getSkippingIndexName
import org.scalatest.matchers.must.Matchers.defined
import org.scalatest.matchers.should.Matchers.{convertToAnyShouldWrapper, the}

import org.apache.spark.sql.{ExplainSuiteHelper, Row}
import org.apache.spark.sql.flint.FlintDataSourceV2.FLINT_DATASOURCE
import org.apache.spark.sql.flint.config.FlintSparkConf
import org.apache.spark.sql.flint.config.FlintSparkConf.CHECKPOINT_MANDATORY

class FlintSparkSkippingIndexSqlITSuite extends FlintSparkSuite with ExplainSuiteHelper {

  /** Test table and index name */
  protected val testTable = s"$catalogName.default.skipping_sql_test"
  protected val testIndex = getSkippingIndexName(testTable)

  override def beforeEach(): Unit = {
    super.beforeAll()

    createPartitionedMultiRowAddressTable(testTable)
  }

  protected override def afterEach(): Unit = {
    super.afterEach()

    deleteTestIndex(testIndex)
    sql(s"DROP TABLE $testTable")
  }

  test("create skipping index with auto refresh") {
    sql(s"""
           | CREATE SKIPPING INDEX ON $testTable
           | (
           |   year PARTITION,
           |   name VALUE_SET,
           |   age MIN_MAX,
           |   address BLOOM_FILTER
           | )
           | WITH (auto_refresh = true)
           | """.stripMargin)

    // Wait for streaming job complete current micro batch
    val job = spark.streams.active.find(_.name == testIndex)
    awaitStreamingComplete(job.get.id.toString)

    val indexData = spark.read.format(FLINT_DATASOURCE).load(testIndex)
    flint.describeIndex(testIndex) shouldBe defined
    indexData.count() shouldBe 2
  }

  test("create skipping index with auto refresh and external scheduler") {
    withTempDir { checkpointDir =>
      setFlintSparkConf(
        FlintSparkConf.CUSTOM_FLINT_SCHEDULER_CLASS,
        "org.opensearch.flint.spark.scheduler.AsyncQuerySchedulerForSqlIT")
      setFlintSparkConf(FlintSparkConf.EXTERNAL_SCHEDULER_ENABLED, true)

      sql(s"""
           | CREATE SKIPPING INDEX ON $testTable
           | ( year PARTITION )
           | WITH (
           |   auto_refresh = true,
           |   scheduler_mode = 'external',
           |   checkpoint_location = '${checkpointDir.getAbsolutePath}'
           | )
           | """.stripMargin)

      val indexData = flint.queryIndex(testIndex)

      flint.describeIndex(testIndex) shouldBe defined
      indexData.count() shouldBe 0

      // query index after 25 sec
      Thread.sleep(25000)
      flint.queryIndex(testIndex).count() shouldBe 2

      // New data won't be refreshed until refresh statement triggered
      sql(s"""
           | INSERT INTO $testTable
           | PARTITION (year=2023, month=5)
           | VALUES ('Hello', 50, 'Vancouver')
           |""".stripMargin)
      flint.queryIndex(testIndex).count() shouldBe 2

      sql(s"DROP SKIPPING INDEX ON $testTable")
      conf.unsetConf(FlintSparkConf.CUSTOM_FLINT_SCHEDULER_CLASS.key)
      conf.unsetConf(FlintSparkConf.EXTERNAL_SCHEDULER_ENABLED.key)
    }
  }

  test("create skipping index with max size value set") {
    sql(s"""
           | CREATE SKIPPING INDEX ON $testTable
           | (
           |   address VALUE_SET(2)
           | )
           | WITH (auto_refresh = true)
           | """.stripMargin)

    val job = spark.streams.active.find(_.name == testIndex)
    awaitStreamingComplete(job.get.id.toString)

    checkAnswer(
      flint.queryIndex(testIndex).select("address"),
      Seq(
        Row("""["Seattle","Portland"]"""),
        Row(null) // Value set exceeded limit size is expected to be null
      ))
  }

  test("create skipping index with non-adaptive bloom filter") {
    sql(s"""
           | CREATE SKIPPING INDEX ON $testTable
           | (
           |   address BLOOM_FILTER(false, 1024, 0.01)
           | )
           | WITH (auto_refresh = true)
           | """.stripMargin)

    val job = spark.streams.active.find(_.name == testIndex)
    awaitStreamingComplete(job.get.id.toString)

    flint.queryIndex(testIndex).count() shouldBe 2

    checkAnswer(sql(s"SELECT name FROM $testTable WHERE address = 'Vancouver'"), Row("Test"))
    sql(s"SELECT name FROM $testTable WHERE address = 'San Francisco'").count() shouldBe 0
  }

  Seq(
    (
      s"CREATE SKIPPING INDEX ON $testTable (address BLOOM_FILTER(20, 0.01))",
      """
        |{
        |   "adaptive": "true",
        |   "num_candidates": "20",
        |   "fpp": "0.01"
        |}
        |""".stripMargin),
    (
      s"CREATE SKIPPING INDEX ON $testTable (address BLOOM_FILTER(false, 100000, 0.001))",
      """
        |{
        |   "adaptive": "false",
        |   "num_items": "100000",
        |   "fpp": "0.001"
        |}
        |""".stripMargin)).foreach { case (query, expectedParamJson) =>
    test(s"create skipping index with bloom filter parameters $expectedParamJson") {
      sql(query)
      val metadata = FlintOpenSearchIndexMetadataService.serialize(
        flint.describeIndex(testIndex).get.metadata())
      val parameters = compact(render(parse(metadata) \\ "parameters"))
      parameters should matchJson(expectedParamJson)
    }
  }

  test("create skipping index with streaming job options") {
    withTempDir { checkpointDir =>
      sql(s"""
             | CREATE SKIPPING INDEX ON $testTable
             | ( year PARTITION )
             | WITH (
             |   auto_refresh = true,
             |   refresh_interval = '5 Seconds',
             |   checkpoint_location = '${checkpointDir.getAbsolutePath}',
             |   extra_options = '{"$testTable": {"maxFilesPerTrigger": "1"}}'
             | )
             | """.stripMargin)

      val index = flint.describeIndex(testIndex)
      index shouldBe defined
      index.get.options.autoRefresh() shouldBe true
      index.get.options.refreshInterval() shouldBe Some("5 Seconds")
      index.get.options.checkpointLocation() shouldBe Some(checkpointDir.getAbsolutePath)
    }
  }

  test("create skipping index with index settings") {
    sql(s"""
           | CREATE SKIPPING INDEX ON $testTable
           | ( year PARTITION )
           | WITH (
           |   index_settings = '{"number_of_shards": 3, "number_of_replicas": 2}'
           | )
           |""".stripMargin)

    // Check if the index setting option is set to OS index setting
    val flintIndexMetadataService =
      new FlintOpenSearchIndexMetadataService(new FlintOptions(openSearchOptions.asJava))

    implicit val formats: Formats = Serialization.formats(NoTypeHints)
    val settings = parse(flintIndexMetadataService.getIndexMetadata(testIndex).indexSettings.get)
    (settings \ "index.number_of_shards").extract[String] shouldBe "3"
    (settings \ "index.number_of_replicas").extract[String] shouldBe "2"
  }

  test("create skipping index with index mappings _source") {
    sql(s"""
           | CREATE SKIPPING INDEX ON $testTable
           | ( year PARTITION )
           | WITH (
           |   index_mappings = '{ "_source": { "enabled": false } }'
           | )
           |""".stripMargin)

    implicit val formats: Formats = Serialization.formats(NoTypeHints)

    val osIndexName = OpenSearchClientUtils.sanitizeIndexName(testIndex)
    val response =
      openSearchClient.indices().get(new GetIndexRequest(osIndexName), RequestOptions.DEFAULT)

    val mapping = response.getMappings.get(osIndexName)
    val indexMappingsOpt = mapping.source.toString
    val mappings = parse(indexMappingsOpt)
    (mappings \ "_source" \ "enabled").extract[Boolean] shouldBe false
  }

  test("create skipping index with index mappings schema merging") {
    sql(s"""
           | CREATE SKIPPING INDEX ON $testTable
           | ( year PARTITION )
           | WITH (
           |   index_mappings = '{ "_source": { "enabled": false }, "properties": { "year": {"index": false} } }'
           | )
           |""".stripMargin)

    val options = new FlintOptions(openSearchOptions.asJava)
    val flintIndexMetadataService =
      new FlintOpenSearchIndexMetadataService(options)
    implicit val formats: Formats = Serialization.formats(NoTypeHints)

    val osIndexName = OpenSearchClientUtils.sanitizeIndexName(testIndex)
    val response =
      openSearchClient.indices().get(new GetIndexRequest(osIndexName), RequestOptions.DEFAULT)

    val mapping = response.getMappings.get(osIndexName)
    val indexMappingsOpt = mapping.source.toString
    val mappings = parse(indexMappingsOpt)
    (mappings \ "_source" \ "enabled").extract[Boolean] shouldBe false

    val flintMetadata =
      flintIndexMetadataService.getIndexMetadata(testIndex)
    val schema = flintMetadata.schema
    val javaMap = schema.asInstanceOf[java.util.HashMap[String, java.util.HashMap[String, Any]]]

    val countMap = javaMap.get("year").asInstanceOf[java.util.Map[String, Any]]
    countMap.get("index") shouldBe false
  }

  Seq(
    "struct_col.field1.subfield VALUE_SET, struct_col.field2 MIN_MAX",
    "`struct_col.field1.subfield` VALUE_SET, `struct_col.field2` MIN_MAX", // ensure previous hack still works
    "`struct_col`.`field1`.`subfield` VALUE_SET, `struct_col`.`field2` MIN_MAX").foreach {
    columnSkipTypes =>
      test(s"build skipping index for nested field $columnSkipTypes") {
        assume(tableType != "iceberg", "ignore iceberg skipping index query rewrite test")

        val testTable = s"$catalogName.default.nested_field_table"
        val testIndex = getSkippingIndexName(testTable)
        withTable(testTable) {
          createStructTable(testTable)
          sql(s"""
             | CREATE SKIPPING INDEX ON $testTable
             | ( $columnSkipTypes )
             | WITH (
             |   auto_refresh = true
             | )
             | """.stripMargin)

          val job = spark.streams.active.find(_.name == testIndex)
          awaitStreamingComplete(job.get.id.toString)

          // Query rewrite nested field
          val query1 = sql(s"SELECT int_col FROM $testTable WHERE struct_col.field2 = 456")
          checkAnswer(query1, Row(40))
          checkKeywordsExistsInExplain(query1, "FlintSparkSkippingFileIndex")

          // Query rewrite deep nested field
          val query2 =
            sql(s"SELECT int_col FROM $testTable WHERE struct_col.field1.subfield = 'value3'")
          checkAnswer(query2, Row(50))
          checkKeywordsExistsInExplain(query2, "FlintSparkSkippingFileIndex")
        }

        deleteTestIndex(testIndex)
      }
  }

  test("create skipping index with invalid option") {
    the[IllegalArgumentException] thrownBy
      sql(s"""
             | CREATE SKIPPING INDEX ON $testTable
             | ( year PARTITION )
             | WITH (autoRefresh = true)
             | """.stripMargin)
  }

  test("create skipping index with auto refresh should fail if mandatory checkpoint enabled") {
    setFlintSparkConf(CHECKPOINT_MANDATORY, "true")
    try {
      the[IllegalArgumentException] thrownBy {
        sql(s"""
               | CREATE SKIPPING INDEX ON $testTable
               | ( year PARTITION )
               | WITH (auto_refresh = true)
               | """.stripMargin)
      }
    } finally {
      setFlintSparkConf(CHECKPOINT_MANDATORY, "false")
    }
  }

  test("create skipping index with full refresh") {
    sql(s"""
         | CREATE SKIPPING INDEX ON $testTable
         | (
         |   year PARTITION,
         |   name VALUE_SET,
         |   age MIN_MAX
         | )
         | """.stripMargin)

    val indexData = spark.read.format(FLINT_DATASOURCE).load(testIndex)

    flint.describeIndex(testIndex) shouldBe defined
    indexData.count() shouldBe 0

    sql(s"REFRESH SKIPPING INDEX ON $testTable")
    indexData.count() shouldBe 2
  }

  test("create skipping index with incremental refresh") {
    withTempDir { checkpointDir =>
      sql(s"""
           | CREATE SKIPPING INDEX ON $testTable
           | ( year PARTITION )
           | WITH (
           |   incremental_refresh = true,
           |   checkpoint_location = '${checkpointDir.getAbsolutePath}'
           | )
           | """.stripMargin)

      // Refresh all present source data as of now
      sql(s"REFRESH SKIPPING INDEX ON $testTable")
      flint.queryIndex(testIndex).count() shouldBe 2

      // New data won't be refreshed until refresh statement triggered
      sql(s"""
           | INSERT INTO $testTable
           | PARTITION (year=2023, month=5)
           | VALUES ('Hello', 50, 'Vancouver')
           |""".stripMargin)
      flint.queryIndex(testIndex).count() shouldBe 2

      sql(s"REFRESH SKIPPING INDEX ON $testTable")
      flint.queryIndex(testIndex).count() shouldBe 3
    }
  }

  test("should fail if refresh an auto refresh skipping index") {
    sql(s"""
           | CREATE SKIPPING INDEX ON $testTable
           | ( year PARTITION )
           | WITH (auto_refresh = true)
           | """.stripMargin)

    assertThrows[IllegalStateException] {
      sql(s"REFRESH SKIPPING INDEX ON $testTable")
    }
  }

  test("create skipping index if not exists") {
    sql(s"""
           | CREATE SKIPPING INDEX
           | IF NOT EXISTS
           | ON $testTable ( year PARTITION )
           | """.stripMargin)
    flint.describeIndex(testIndex) shouldBe defined

    // Expect error without IF NOT EXISTS, otherwise success
    assertThrows[IllegalStateException] {
      sql(s"""
             | CREATE SKIPPING INDEX
             | ON $testTable ( year PARTITION )
             | """.stripMargin)
    }
    sql(s"""
           | CREATE SKIPPING INDEX
           | IF NOT EXISTS
           | ON $testTable ( year PARTITION )
           | """.stripMargin)
  }

  test("create skipping index with quoted table and column name") {
    sql(s"""
           | CREATE SKIPPING INDEX ON `$catalogName`.`default`.`skipping_sql_test`
           | (
           |   `year` PARTITION,
           |   `name` VALUE_SET,
           |   `age` MIN_MAX
           | )
           | """.stripMargin)

    val index = flint.describeIndex(testIndex)
    index shouldBe defined

    val metadata = index.get.metadata()
    metadata.source shouldBe testTable
    metadata.indexedColumns.map(_.asScala("columnName")) shouldBe Seq("year", "name", "age")
  }

  test("describe skipping index") {
    flint
      .skippingIndex()
      .onTable(testTable)
      .addPartitions("year")
      .addValueSet("name")
      .addMinMax("age")
      .create()

    val result = sql(s"DESC SKIPPING INDEX ON $testTable")

    checkAnswer(
      result,
      Seq(
        Row("year", "int", "PARTITION"),
        Row("name", "string", "VALUE_SET"),
        Row("age", "int", "MIN_MAX")))
  }

  test("create skipping index on table without database name") {
    sql("CREATE SKIPPING INDEX ON skipping_sql_test ( year PARTITION )")

    flint.describeIndex(testIndex) shouldBe defined
  }

  test("create skipping index on table in other database") {
    sql("CREATE SCHEMA sample")
    sql("USE sample")

    // Create index without database name specified
    sql(s"CREATE TABLE test1 (name STRING) USING $tableType")
    sql("CREATE SKIPPING INDEX ON test1 (name VALUE_SET)")

    // Create index with database name specified
    sql(s"CREATE TABLE test2 (name STRING) USING $tableType")
    sql("CREATE SKIPPING INDEX ON sample.test2 (name VALUE_SET)")

    try {
      flint.describeIndex(s"flint_${catalogName}_sample_test1_skipping_index") shouldBe defined
      flint.describeIndex(s"flint_${catalogName}_sample_test2_skipping_index") shouldBe defined
    } finally {

      /**
       * TODO: REMOVE DROP TABLE when iceberg support CASCADE. More reading at
       * https://github.com/apache/iceberg/pull/7275.
       */
      if (tableType.equalsIgnoreCase("iceberg")) {
        sql("DROP TABLE test1")
        sql("DROP TABLE test2")
      }
      sql("DROP DATABASE sample CASCADE")
    }
  }

  test("create skipping index on table in other database than current") {
    sql("CREATE SCHEMA sample")
    sql("USE sample")

    // Specify database "default" in table name instead of current "sample" database
    sql(s"CREATE SKIPPING INDEX ON $testTable (name VALUE_SET)")

    try {
      flint.describeIndex(testIndex) shouldBe defined
    } finally {
      sql("DROP DATABASE sample CASCADE")
    }
  }

  test("should return empty if no skipping index to describe") {
    val result = sql(s"DESC SKIPPING INDEX ON $testTable")

    checkAnswer(result, Seq.empty)
  }

  test("update full refresh skipping index to auto refresh") {
    sql(s"""
         | CREATE SKIPPING INDEX ON $testTable
         | (
         |   year PARTITION,
         |   name VALUE_SET,
         |   age MIN_MAX
         | )
         | """.stripMargin)

    flint.describeIndex(testIndex) shouldBe defined
    flint.queryIndex(testIndex).count() shouldBe 0

    sql(s"""
         | ALTER SKIPPING INDEX ON $testTable
         | WITH (auto_refresh = true)
         | """.stripMargin)

    // Wait for streaming job complete current micro batch
    val job = spark.streams.active.find(_.name == testIndex)
    awaitStreamingComplete(job.get.id.toString)
    flint.queryIndex(testIndex).count() shouldBe 2
  }

  test("update incremental refresh skipping index to auto refresh") {
    withTempDir { checkpointDir =>
      sql(s"""
           | CREATE SKIPPING INDEX ON $testTable
           | ( year PARTITION )
           | WITH (
           |   incremental_refresh = true,
           |   checkpoint_location = '${checkpointDir.getAbsolutePath}'
           | )
           | """.stripMargin)

      // Refresh all present source data as of now
      sql(s"REFRESH SKIPPING INDEX ON $testTable")
      flint.queryIndex(testIndex).count() shouldBe 2

      // New data will be refreshed after updating index to auto refresh
      sql(s"""
           | INSERT INTO $testTable
           | PARTITION (year=2023, month=5)
           | VALUES ('Hello', 50, 'Vancouver')
           |""".stripMargin)

      sql(s"""
           | ALTER SKIPPING INDEX ON $testTable
           | WITH (
           |   auto_refresh = true,
           |   incremental_refresh = false
           | )
           | """.stripMargin)

      // Wait for streaming job complete current micro batch
      val job = spark.streams.active.find(_.name == testIndex)
      awaitStreamingComplete(job.get.id.toString)
      flint.queryIndex(testIndex).count() shouldBe 3
    }
  }

  test("update auto refresh skipping index to full refresh") {
    sql(s"""
         | CREATE SKIPPING INDEX ON $testTable
         | ( year PARTITION )
         | WITH (auto_refresh = true)
         | """.stripMargin)

    // Wait for streaming job complete current micro batch
    val job = spark.streams.active.find(_.name == testIndex)
    awaitStreamingComplete(job.get.id.toString)

    flint.describeIndex(testIndex) shouldBe defined
    flint.queryIndex(testIndex).count() shouldBe 2

    sql(s"""
         | ALTER SKIPPING INDEX ON $testTable
         | WITH (auto_refresh = false)
         | """.stripMargin)

    spark.streams.active.find(_.name == testIndex) shouldBe empty

    // New data won't be refreshed until refresh statement triggered
    sql(s"""
         | INSERT INTO $testTable
         | PARTITION (year=2023, month=5)
         | VALUES ('Hello', 50, 'Vancouver')
         |""".stripMargin)
    flint.queryIndex(testIndex).count() shouldBe 2

    sql(s"REFRESH SKIPPING INDEX ON $testTable")
    flint.queryIndex(testIndex).count() shouldBe 3
  }

  test("update auto refresh skipping index to incremental refresh") {
    withTempDir { checkpointDir =>
      sql(s"""
           | CREATE SKIPPING INDEX ON $testTable
           | ( year PARTITION )
           | WITH (
           |   auto_refresh = true,
           |   checkpoint_location = '${checkpointDir.getAbsolutePath}'
           | )
           | """.stripMargin)

      // Wait for streaming job complete current micro batch
      val job = spark.streams.active.find(_.name == testIndex)
      awaitStreamingComplete(job.get.id.toString)

      flint.describeIndex(testIndex) shouldBe defined
      flint.queryIndex(testIndex).count() shouldBe 2

      sql(s"""
           | ALTER SKIPPING INDEX ON $testTable
           | WITH (
           |   auto_refresh = false,
           |   incremental_refresh = true
           | )
           | """.stripMargin)

      spark.streams.active.find(_.name == testIndex) shouldBe empty

      // New data won't be refreshed until refresh statement triggered
      sql(s"""
           | INSERT INTO $testTable
           | PARTITION (year=2023, month=5)
           | VALUES ('Hello', 50, 'Vancouver')
           |""".stripMargin)
      flint.queryIndex(testIndex).count() shouldBe 2

      sql(s"REFRESH SKIPPING INDEX ON $testTable")
      flint.queryIndex(testIndex).count() shouldBe 3
    }
  }

  test("drop and vacuum skipping index") {
    flint
      .skippingIndex()
      .onTable(testTable)
      .addPartitions("year")
      .create()

    sql(s"DROP SKIPPING INDEX ON $testTable")
    sql(s"VACUUM SKIPPING INDEX ON $testTable")
    flint.describeIndex(testIndex) shouldBe empty
  }

  test("vacuum skipping index with checkpoint") {
    withTempDir { checkpointDir =>
      flint
        .skippingIndex()
        .onTable(testTable)
        .addPartitions("year")
        .options(
          FlintSparkIndexOptions(
            Map(
              "auto_refresh" -> "true",
              "checkpoint_location" -> checkpointDir.getAbsolutePath)),
          testIndex)
        .create()
      flint.refreshIndex(testIndex)

      val job = spark.streams.active.find(_.name == testIndex)
      awaitStreamingComplete(job.get.id.toString)
      flint.deleteIndex(testIndex)

      // Checkpoint folder should be removed after vacuum
      checkpointDir.exists() shouldBe true
      sql(s"VACUUM SKIPPING INDEX ON $testTable")
      flint.describeIndex(testIndex) shouldBe empty
      checkpointDir.exists() shouldBe false
    }
  }

  test("analyze skipping index with for supported data types") {
    val result = sql(s"ANALYZE SKIPPING INDEX ON $testTable")

    checkAnswer(
      result,
      Seq(
        Row(
          "year",
          "integer",
          "PARTITION",
          "PARTITION data structure is recommended for partition columns"),
        Row(
          "month",
          "integer",
          "PARTITION",
          "PARTITION data structure is recommended for partition columns"),
        Row(
          "name",
          "string",
          "BLOOM_FILTER",
          "BLOOM_FILTER data structure is recommended for StringType columns"),
        Row(
          "age",
          "integer",
          "MIN_MAX",
          "MIN_MAX data structure is recommended for IntegerType columns"),
        Row(
          "address",
          "string",
          "BLOOM_FILTER",
          "BLOOM_FILTER data structure is recommended for StringType columns")))
  }

  test("analyze skipping index on invalid table") {
    the[IllegalStateException] thrownBy {
      sql(s"ANALYZE SKIPPING INDEX ON testTable")
    }
  }
}
