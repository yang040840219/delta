package com.mine

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.delta.{DeltaConfig, DeltaConfigs, DeltaLog}
import org.apache.spark.sql.delta.catalog.DeltaCatalog
import org.apache.spark.sql.internal.SQLConf
import org.scalatest.BeforeAndAfterEach


class CleanLogSuite extends SparkFunSuite with BeforeAndAfterEach with PredicateHelper {

	val spark = SparkSession.builder().master("local[2]")
		.config("spark.network.timeout", "1000000s")
		.config("spark.executor.heartbeatInterval", "100000s")
		.config("spark.storage.blockManagerSlaveTimeoutMs", "100000s")
		.config("spark.sql.shuffle.partitions", 3)
		.config("spark.default.parallelism", 2)
		.config("spark.databricks.delta.snapshotPartitions", 1)
		.config("spark.databricks.delta.properties.defaults.logRetentionDuration", "2 day")
  	.config("spark.databricks.delta.properties.defaults.sampleRetentionDuration", "1 day")
		.config("hive.metastore.warehouse.dir", "/opt/data/warehouse")
		.config("hive.exec.dynamic.partition.mode", "nonstrict")
		.config("spark.sql.hive.convertMetastoreParquet", false)
		.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
		.config("spark.sql.catalog.spark_catalog",
			"org.apache.spark.sql.delta.catalog.DeltaCatalog")
		.config(SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION.key, classOf[DeltaCatalog].getName)
		.enableHiveSupport()
		.getOrCreate()


	val json = "file:///opt/data/delta/json/realtime"
	val path = "file:///opt/data/delta/realtime_log"

	test("clean delta log") {
		val deltaLog = DeltaLog.forTable(spark, "file:///opt/data/delta/realtime_log")
		// scalastyle:off
		println(DeltaConfigs.LOG_RETENTION.key)

		deltaLog.doLogCleanup()
	}

	test("delta write") {
		val df = spark.read.json(json)
		df.write.mode(SaveMode.Overwrite).format("delta").save(path)
	}

}
