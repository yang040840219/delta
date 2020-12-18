// scalastyle:off
package com.mine

import java.util.concurrent.TimeUnit

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.delta.util.JsonUtils
import org.apache.spark.sql.functions.{col, from_json, schema_of_json, struct}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types.{DataTypes, IntegerType, StringType, StructField, StructType}

class StreamingSuite extends SparkFunSuite {

	test("streaming schema evolution") {
		val spark = SparkSession.builder().master("local[4]")
			.appName("streaming schema evolution")
			.config(SQLConf.SHUFFLE_PARTITIONS.key, 3)
			.config(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "false")
			.config(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "false")
  		.config("spark.sql.streaming.schemaInference", "false")
			.getOrCreate()

		import spark.implicits._

		val lines = spark.readStream.format("socket").option("host", "localhost")
			.option("port", "9999").load().as[String]

		val schema = new StructType(Array(
			new StructField("name", StringType),
			new StructField("age", IntegerType),
			new StructField("age", IntegerType)
		))

		val df = lines.select(from_json(col("value"), schema).as("data"))
			.select("data.*")

		val filter = df

		val query = filter.writeStream
  			.foreachBatch({
				  (batchDF: DataFrame, batchId: Long) => {
					   batchDF.show(truncate = false)
				  }
			  })
			.start()

		query.awaitTermination()


	}

}
