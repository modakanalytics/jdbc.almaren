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
  insertUpdateTable(insertQuery, insertSourceDf)

  test(insertSourceDf, getPostgresTable("select * from public.person_info"), "jdbc Batch insert test")

  val updateSourceDf = Seq(
    ("John", "Jones", "London"),
    ("David", "Smith", "Russia"),
    ("Michael", "Lee", "Indonesia"),
    ("Chris", "Johnson", "India"),
    ("Mike", "Brown", "Brazil")
  ).toDF("first_name", "last_name", "country")

  val updateQuery = "UPDATE public.person_info set first_name = ?, last_name = ? where country = ?"
  insertUpdateTable(updateQuery, updateSourceDf)

  test(updateSourceDf, getPostgresTable("select * from public.person_info"), "jdbc Batch update test")

  executeJdbcQuery("TRUNCATE TABLE public.person_info")
  insertUpdateTable("INSERT INTO public.person_info_temp (first_name, last_name, country) VALUES(?,?,?)", insertSourceDf)

  val mergeQuery =
    """WITH upsert as(
      |  update
      |    public.person_info t2
      |  set
      |    first_name = t1.first_name,
      |    last_name = t1.last_name
      |  from
      |    person_info_temp t1
      |  where
      |    t2.country = t1.country RETURNING t2.*
      |)
      |insert into
      |  person_info
      |select
      |  p.first_name,
      |  p.last_name,
      |  p.country
      |from
      |  person_info_temp p
      |where
      |  p.country not in (
      |    select
      |      q.country
      |    from
      |      upsert q
      |  );""".stripMargin

  executeJdbcQuery(mergeQuery)

  test(insertSourceDf, getPostgresTable("select * from public.person_info"), "jdbc Query test")

  executeJdbcQuery("TRUNCATE TABLE public.person_info")
  executeJdbcQuery("TRUNCATE TABLE public.person_info_temp")

  def insertUpdateTable(sqlQuery: String, df: DataFrame): Long = {
    almaren.builder
      .sourceDataFrame(df)
      .sqlExpr("monotonically_increasing_id() as __ID__","*")
      .jdbcBatch("jdbc:postgresql://localhost:5432/almaren", "org.postgresql.Driver", sqlQuery, 1000, Some("postgres"), Some("postgres"),Map("connectionTimeoutMillis" -> "3000","maxSize"->"10"))
      .batch
      .count
  }

  def executeJdbcQuery(query: String): DataFrame = {
    almaren.builder
      .jdbcQuery("jdbc:postgresql://localhost:5432/almaren", "org.postgresql.Driver", query, Some("postgres"), Some("postgres"),Map("connectionTimeoutMillis" -> "3000","maxSize"->"10"))
      .batch
  }

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
