package com.mine

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

object Main {

    def main(args: Array[String]): Unit = {
        val path = "/data/delta/t_users"
        val spark = SparkSession.builder().master("local[2]")
        .config("spark.network.timeout", "1000000s")
        .config("spark.executor.heartbeatInterval", "100000s")
        .config("spark.sql.shuffle.partitions", 3)
        .config("spark.default.parallelism", 2)
        .config("spark.databricks.delta.snapshotPartitions", 1)
        .config("hive.metastore.warehouse.dir", "/data/warehouse").getOrCreate()

        import spark.implicits._

        val reader = spark.readStream.format("delta")
        val df = reader.load(path)
        val query = df.groupBy($"p_day").count().as("user_cnt").writeStream
                .outputMode(OutputMode.Complete())
                 .format("console")
                .trigger(Trigger.ProcessingTime(5, TimeUnit.SECONDS))
                .start()
        query.awaitTermination()
        spark.close()
        spark.stop()
    }

}
