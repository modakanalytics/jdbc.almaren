package com.github.music.of.the.ainur.almaren.jdbc

import org.apache.spark.sql.{AnalysisException, Column, DataFrame, SaveMode, SparkSession}
import org.scalatest._
import org.apache.spark.sql.functions._
import com.github.music.of.the.ainur.almaren.Almaren
import com.github.music.of.the.ainur.almaren.builder.Core.Implicit
import com.github.music.of.the.ainur.almaren.jdbc.JDBC.JDBCImplicit

class Test extends FunSuite with BeforeAndAfter {
  val almaren = Almaren("jdbc-almaren")

  val spark: SparkSession = almaren.spark
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "1")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")


  import spark.implicits._

  val insertSourceDf = Seq(
    ("John", "Smith", "London"),
    ("David", "Jones", "India"),
    ("Michael", "Johnson", "Indonesia"),
    ("Chris", "Lee", "Brazil"),
    ("Mike", "Brown", "Russia")
  ).toDF("first_name", "last_name", "country")


  val insertQuery = "INSERT INTO public.person_info (first_name, last_name, country) VALUES(?,?,?)"

  val df2 = almaren.builder
    .sourceDataFrame(insertSourceDf)
    .sql("select monotonically_increasing_id() as __ID__,* from __TABLE__")
    .jdbcBatch("jdbc:postgresql://localhost:5432/almaren", "org.postgresql.Driver", insertQuery, 1000, Some("postgres"), Some("postgres"))
    .batch

  //performing action for the query execution
  df2.count

  val insertFinalDf = getPostgresTable("select * from person_info")

  test(insertSourceDf, insertFinalDf, "jdbc Batch insert test")

  val updateSourceDf = Seq(
    ("John", "Jones"),
    ("David", "Smith"),
    ("Michael", "Lee"),
    ("Chris", "Johnson"),
    ("Mike", "Brown")
  ).toDF("first_name", "last_name")

  val updateQuery = "UPDATE person_info set first_name = ? where last_name = ?"

  val df3 = almaren.builder
    .sourceDataFrame(updateSourceDf)
    .sql("select monotonically_increasing_id() as __ID__,first_name,last_name from __TABLE__")
    .jdbcBatch("jdbc:postgresql://localhost:5432/almaren", "org.postgresql.Driver", updateQuery, 1000, Some("postgres"), Some("postgres"))
    .batch

  //performing action for the query execution
  df3.count

  val updateFinalDf = getPostgresTable("select first_name,last_name from person_info")

  test(updateSourceDf, updateFinalDf, "jdbc Batch update test")

  def getPostgresTable(query: String): DataFrame = {
    almaren.builder
      .sourceJdbc("jdbc:postgresql://localhost:5432/almaren", "org.postgresql.Driver", query, Some("postgres"), Some("postgres"))
      .batch
  }


  def test(df1: DataFrame, df2: DataFrame, name: String): Unit = {
    testCount(df1, df2, name)
    testCompare(df1, df2, name)
  }

  def testCount(df1: DataFrame, df2: DataFrame, name: String): Unit = {
    val count1 = df1.count()
    val count2 = df2.count()
    val count3 = spark.emptyDataFrame.count()
    test(s"Count Test:$name should match") {
      assert(count1 == count2)
    }
    test(s"Count Test:$name should not match") {
      assert(count1 != count3)
    }
  }

  // Doesn't support nested type and we don't need it :)
  def testCompare(df1: DataFrame, df2: DataFrame, name: String): Unit = {
    val diff = compare(df1, df2)
    test(s"Compare Test:$name should be zero") {
      assert(diff == 0)
    }
    test(s"Compare Test:$name, should not be able to join") {
      assertThrows[AnalysisException] {
        compare(df2, spark.emptyDataFrame)
      }
    }
  }

  private def compare(df1: DataFrame, df2: DataFrame): Long =
    df1.as("df1").join(df2.as("df2"), joinExpression(df1), "leftanti").count()

  private def joinExpression(df1: DataFrame): Column =
    df1.schema.fields
      .map(field => col(s"df1.${field.name}") <=> col(s"df2.${field.name}"))
      .reduce((col1, col2) => col1.and(col2))

  after {
    spark.stop
  }
}