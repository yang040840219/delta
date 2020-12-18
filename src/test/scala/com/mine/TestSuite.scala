package com.mine

import java.util.Calendar

import io.delta.tables.DeltaTable
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Alias, If, PredicateHelper}
import org.apache.spark.sql.delta.catalog.DeltaCatalog
import org.apache.spark.sql.delta.{DeltaConfigs, DeltaLog}
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.apache.spark.sql.{Encoders, _}
import org.scalatest.BeforeAndAfterEach
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.internal.SQLConf

import scala.collection.mutable


case class User(id: Int,
                name: String,
                age: Int,
                p_day: String)

case class SpecialUser(id: Int,
                       name: String,
                       age: Int,
                       p_day: String,
                       is_help: Boolean
                      )

case class Student(id: Int,
                   name: String,
                   p_day: String)


case class AddressBook(
	                      owner: String,
	                      ownerPhoneNumbers: String,
	                      contacts: List[Contacts]
                      )

case class Contacts(name: String, phoneNumber: Option[String])

case class AddressBook1(
	                       owner: String,
	                       ownerPhoneNumbers: String,
	                       contacts: Map[String, String]
                       )

case class AddressBook2(
	                       owner: String,
	                       ownerPhoneNumbers: Array[String],
	                       remark: Option[String],
	                       contacts: Array[Contacts]
                       )


class TestSuite extends SparkFunSuite with BeforeAndAfterEach with PredicateHelper {

	val path = "/data/delta/t_users"
	val studentPath = "/data/delta/t_students"
	val addressPath = "/data/delta/t_address_book"
	val addressPath1 = "/data/delta/t_address_book_1"
	val addressPath2 = "/data/delta/t_address_book_2"


	val spark = SparkSession.builder().master("local[2]")
		.config("spark.network.timeout", "1000000s")
		.config("spark.executor.heartbeatInterval", "100000s")
		.config("spark.storage.blockManagerSlaveTimeoutMs", "100000s")
		.config("spark.sql.shuffle.partitions", 3)
		.config("spark.default.parallelism", 1)
		.config("spark.databricks.delta.snapshotPartitions", 1)
		.config("spark.databricks.delta.properties.defaults.logRetentionDuration", "1 hours")
		.config("hive.metastore.warehouse.dir", "/data/warehouse")
		.config("hive.exec.dynamic.partition.mode", "nonstrict")
		.config("spark.sql.hive.convertMetastoreParquet", false)
		.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
		.config("spark.sql.catalog.spark_catalog",
			"org.apache.spark.sql.delta.catalog.DeltaCatalog")
		.config(SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION.key, classOf[DeltaCatalog].getName)
		.enableHiveSupport()
		.getOrCreate()

	spark.sparkContext.setLogLevel("info")

	val hiveSite = new Path("/data/server/apache-hive-2.3.7-bin/conf/hive-site.xml")

	// scalastyle:off
	spark.sparkContext.hadoopConfiguration.addResource(hiveSite)

	import spark.implicits._

	override protected def afterEach(): Unit = {
		super.afterEach()
		spark.stop()
	}

	// scalastyle:on
	test("write t_address_book_2 1") {
		val address1 = AddressBook2("owner1", Array("131", "132"), Some("home"),
			Array(Contacts("contact-1", Some("131"))))
		val data = spark.createDataFrame(Seq(address1))
		val merge = data.withColumn("p_day", lit("2019-08-15"))
		// merge.write.format("parquet").mode(SaveMode.Overwrite)
		// .partitionBy("p_day").save(addressPath2)
		// merge.write.format("hive").option("fileFormat", "parquet")
		// .mode(SaveMode.Overwrite).partitionBy("p_day").saveAsTable("t_address_2")
	}


	test("write t_address_book_2") {
		val address1 = AddressBook2("owner1", Array("131", "131"), Some("home1"),
			Array(Contacts("contact-1", Some("131"))))

		val address2 = AddressBook2("owner2", Array("132", "132"), None,
			Array(Contacts("contact-2", Some("132"))))

		val address3 = AddressBook2("owner3", Array("133", "133"), Some("home3"),
			Array(Contacts("contact-3", None), Contacts("contact-3", None)))

		val contactSchema = Encoders.product[Contacts].schema

		val customSchema = StructType(
			Array(StructField("owner", StringType, nullable = false),
				StructField("ownerPhoneNumbers",
					ArrayType(StringType, containsNull = false), nullable = false),
				StructField("remark", StringType, nullable = true),
				StructField("contacts", ArrayType(StructType(
					Seq(StructField("name", StringType, nullable = false),
						StructField("phoneNumber", StringType, nullable = true))),
					containsNull = false), nullable = false)
				//                StructField("contacts", ArrayType(contactSchema), true)
			)
		)
		val rowRDD = spark.sparkContext.parallelize(List(address1), 1).map(book => {
			Row(book.owner, book.ownerPhoneNumbers, book.remark, book.contacts)
		})
		// scalastyle:off
		rowRDD.collect().foreach(println)
		//        val expected = spark.createDataFrame(rowRDD, customSchema)
		val data = Seq(address1, address2, address3).toDF()
		data.rdd.collect().foreach(println)
		//        data.printSchema()
		val expected = spark.createDataFrame(data.rdd, customSchema).repartition(1)
		expected.printSchema()
		expected.select("owner", "contacts").show(truncate = false)
		//expected.write.format("delta").mode(SaveMode.Overwrite).save(addressPath2)
	}

	test("read t_address_book_2 schema") {
		val df = spark.read.format("delta").parquet(addressPath2)
		df.printSchema()
	}

	test("write t_address_book") {

		val a1 = AddressBook("a", "123", List(Contacts("b", Some("234"))))
		val data = spark.createDataFrame(List(a1))
		data.write.format("delta").mode(SaveMode.Overwrite).save(addressPath)
	}

	test("write t_address_book_1") {

		val a1 = AddressBook1("a", "123", Map("b" -> "234"))
		val a2 = AddressBook1("x", "234", Map("c" -> "434"))
		val a3 = AddressBook1("f", "456", Map("d" -> "789"))
		val data = spark.createDataFrame(List(a1, a2, a3))
		data.write.format("delta").mode(SaveMode.Overwrite).save(addressPath1)
	}

	test("write student data 1") {
		val s1 = Student(1, "s1", "2019-06-01")
		val data = spark.createDataFrame(List[Student](s1))
		data.write.format("delta").mode(SaveMode.Overwrite).partitionBy("p_day").save(studentPath)
	}


	test("write special user data") {
		val u3 = SpecialUser(3, "c", 15, "2019-04-02", false)
		val data = spark.createDataFrame(List[SpecialUser](u3))
		data.write.format("delta")
			.option("mergeSchema", "true")
			.mode(SaveMode.Append).partitionBy("p_day").save(path)
	}

	test("update data") {
		val u1 = User(1, "a1", 10, "2019-04-01")
		val data = spark.createDataFrame(List[User](u1))
		data.write.format("delta").mode(SaveMode.Overwrite)
			.option("replaceWhere", "id=1")
			.partitionBy("p_day").save(path)
	}

	test("read old version") {
		val data = spark.read.format("delta").option("versionAsOf", 10).load(path)
		data.explain(true)
		data.show()
	}

	test("read with timestamp") {
		val calendar = Calendar.getInstance()
		calendar.setTimeInMillis(1557906900455L)
		val format = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss SSS")
		val timestamp = format.format(calendar.getTime)
		println(timestamp)
		val reader = spark.read.format("delta").option("timestampAsOf", "2019-05-29")
		val df = reader.load(path)
		df.show()
	}


	test("write with timestamp 0") {
		val u1 = User(7, "a", 13, "2019-04-13")
		val u2 = User(8, "b", 14, "2019-04-13")
		val data = spark.createDataFrame(List[User](u1, u2))
		data.write.format("delta").mode(SaveMode.Overwrite).partitionBy("p_day").save(path)
	}


	test("write data") {
		val u1 = User(1, "a", 10, "2019-04-01")
		val u2 = User(2, "b", 12, "2019-04-01")
		val data = spark.createDataFrame(List[User](u1, u2))
		data.write.format("delta").mode(SaveMode.Overwrite).save(path)
	}

	test("read data") {
		// val data = spark.read.format("delta").load(path)
		val u1 = User(1, "a", 10, "2019-04-01")
		val u2 = User(2, "b", 12, "2019-04-01")
		val data = spark.createDataFrame(List[User](u1, u2))
		data.show()
		data.write.format("delta").mode(SaveMode.Overwrite).saveAsTable("t_user_delta")
	}

	test("update with condition") {
		val table = DeltaTable.forPath(spark, path)
		import spark.implicits._
		table.update($"id" < 50, Map("age" -> $"id" / 2))
		table.toDF.show()
	}


	test("construct with df") {
		//val data = spark.read.format("delta").load(path)
		val deltaTable = DeltaTable.forPath(spark, path)
		deltaTable.toDF.show()
		deltaTable.update($"id" === 35, Map("name" -> functions.col("world")))
		deltaTable.toDF.show()
	}

	test("delete with condition") {
		val c = functions.expr("name == 'a' and p_day = '2019-04-13'")

		val expr = c.expr

		val references = c.expr.references

		println(s"column:$c")
		println(s"expr:${expr.dataType}")
		println(s"references:$references")

		references.foreach(attribute => println(attribute.getClass))

		val expressions = splitConjunctivePredicates(c.expr)
		println(expressions)
	}

	test("manual update df") {
		val df1 = Seq.apply(("a", 1), ("b", 2), ("c", 3)).toDF("name", "age")
		df1.show()

		println("column expr", $"name".expr)

		val condition = ($"name" === "c").expr
		val setExpressions = Seq($"name".expr, ($"age" * 2).expr) // 一次可能更新多个列
		val logicalPlan = df1.queryExecution.logical
		setExpressions.zip(logicalPlan.output).foreach(println)
		val updateColumns = setExpressions.zip(logicalPlan.output)
			.map { case (update, original) =>
				val updated = If(condition, update, original)
				new Column(Alias(updated, original.name)())
			}
		val target = df1.select(updateColumns: _*)
		target.show()
	}

	test("merge") {
		val deltaTable = DeltaTable.forPath(spark, path)
		val mergedDF = Seq(User(1, "a1", 10, "2019-07-24"),
			User(100, "a100", 100, "2019-07-24")).toDF("id", "name", "age", "p_day")

		val data = deltaTable.toDF

		deltaTable.merge(mergedDF, mergedDF("id") === data("id"))
			.whenMatched().update(Map("id" -> mergedDF("id"), "name" -> mergedDF("name"),
			"age" -> mergedDF("age"), "p_day" -> mergedDF("p_day")))
			.whenNotMatched().insert(Map("id" -> mergedDF("id"), "name" -> mergedDF("name"),
			"age" -> mergedDF("age"), "p_day" -> mergedDF("p_day"),
			"is_help" -> functions.lit("false")))
			.execute()

		deltaTable.toDF.show()
	}

	test("merge all") {
		val deltaTable = DeltaTable.forPath(spark, path)
		val data = deltaTable.toDF
		data.show()

		//deltaTable.delete($"id" >= 45)

		val source = Seq(SpecialUser(2, "ay", 119, "2019-07-24", true),
			SpecialUser(1, "am", 110, "2019-07-24", true)
		).toDF("id", "name", "age", "p_day", "is_help")

		deltaTable.merge(source, data("id") === source("id"))
			.whenMatched(data("id") === 1).updateAll()
			.whenMatched().delete()
			.whenNotMatched().insertAll()
			.execute()

		data.show()
	}

	test("simple catalyst") {
		val attribute = UnresolvedAttribute.quotedString("t1.id")
		println(attribute.nameParts)
	}


	test("simple hive table read") {
		//spark.sparkContext.setLogLevel("TRACE")
		//val df = spark.read.table("db_tmp.t_u_2")
		val df1 = Seq((1, "a", 10, "a1")).toDF("id", "name", "age", "address")
		df1.write.mode("append").saveAsTable("db_tmp.t_u_2")
	}

	test("spark code gen") {
		spark.sql("select name,avg(age) from db_tmp.t_u_2 group by name").queryExecution.debug.codegen()
	}

	test("enableFullRetentionRollback") {
		val deltaLog = DeltaLog.forTable(spark, path)
		println(deltaLog.enableExpiredLogCleanup)
		val metadata = deltaLog.snapshot.metadata
		println(metadata)
		val interval = DeltaConfigs.LOG_RETENTION.fromMetaData(metadata)
		println(interval)
	}

	test("create table") {
		val sql =
			s"""
				 |create table t_user_delta(
				 |age int,
				 |id int,
				 |name string,
				 |p_day string)
				 |using delta
				 |partitioned by (p_day)
				 |location "$path"
             """.stripMargin
		spark.sql(sql)

	}

	test("query table") {
		val sql =
			s"""
				 |show databases
             """.stripMargin

		spark.sql(sql).show(truncate = false)
	}

}
