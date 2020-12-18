package com.mine

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.delta.DeltaOptions
import org.apache.spark.sql.delta.catalog.DeltaCatalog
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.JsonUtils
import org.apache.spark.sql.internal.SQLConf
import org.scalatest.BeforeAndAfterEach
import org.apache.spark.sql.functions._

class SchemaEvolutionSuite extends SparkFunSuite with BeforeAndAfterEach with PredicateHelper {


	override protected def afterAll(): Unit = {
		spark.stop()
	}

	val spark = SparkSession.builder().master("local[2]")
		.config("spark.network.timeout", "1000000s")
		.config("spark.executor.heartbeatInterval", "100000s")
		.config("spark.storage.blockManagerSlaveTimeoutMs", "100000s")
		.config("spark.sql.shuffle.partitions", 3)
		.config("spark.default.parallelism", 1)
		.config("spark.databricks.delta.snapshotPartitions", 1)
		.config("spark.databricks.delta.properties.defaults.logRetentionDuration", "6 days")
		.config("hive.metastore.warehouse.dir", "/data/warehouse")
		.config("hive.exec.dynamic.partition.mode", "nonstrict")
		.config("spark.sql.hive.convertMetastoreParquet", false)
		.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
		.config("spark.sql.catalog.spark_catalog",
			"org.apache.spark.sql.delta.catalog.DeltaCatalog")
  	.config(DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE.key, false)
		.config(SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION.key, classOf[DeltaCatalog].getName)
		.enableHiveSupport()
		.getOrCreate()

	import spark.implicits._

	case class One(a: String, b: String)

	case class Two(a: String, b: String, c: String)

	val path = "/opt/data/spark/delta/evolution"

	test("x") {
		// scalastyle:off

		import spark.implicits._

		val df = spark.createDataFrame(
			Seq((1, "{\"a\":\"10\"}"))
		).toDF("id", "value")


	 val x = df.select("value").map(line => {
		 val map = JsonUtils.fromJson[Map[String, String]](line.getString(0))
		 println(map.mkString(","))
		 map
	 })

	 x.show()

	 x.printSchema()
	}

	test("save One") {
		val one1 = One("hello1", "one")
		val one2 = One("hello2", "one")
		val df = spark.createDataFrame(Seq.apply(one1, one2))
		df.printSchema()
		df.write.mode(SaveMode.Overwrite).format("delta").save(path)
	}

	test("save Two") {
		val two1 = Two("word1", "two", "two")
		val two2 = Two("word2", "two", "two")
		val df = spark.createDataFrame(Seq.apply(two1, two2))
		df.printSchema()
		df.write.mode(SaveMode.Append)
			.option(DeltaOptions.MERGE_SCHEMA_OPTION, "true")
			.format("delta").save(path)
	}

	test("read schema") {
		val df = spark.read.format("delta").load(path)
		df.show(truncate = false)
		df.printSchema()
	}


	test("save Two with partition") {
		val two1 = Two("word1", "two", "two")
		val two2 = Two("word2", "two", "two")
		val df = spark.createDataFrame(Seq.apply(two1, two2))
		df.printSchema()
		df.write.mode(SaveMode.Append).partitionBy("c")
			.format("delta")
			.save(path)
	}


	test("read evolution") {
		val df = spark.read.format("delta").load(path)
		df.printSchema()
		df.show(truncate = false)
	}

}
