package com.github.music.of.the.ainur.almaren.jdbc

import java.util.concurrent.Executors

import com.github.music.of.the.ainur.almaren.Tree
import com.github.music.of.the.ainur.almaren.builder.Core
import com.github.music.of.the.ainur.almaren.state.core.Main
import org.apache.spark.sql.{DataFrame, Row}
import scalikejdbc._

import scala.util.{Failure, Success, Try}

private[almaren] case class MainJDBC(url: String, driver: String, query: String, user: Option[String], password: Option[String], params: Map[String, String]) extends Main {

  override def core(df: DataFrame): DataFrame = {
    logger.info(s"url:{$url}, driver:{$driver}, query:{$query}, user:{$user}, params:{$params}")
    df
  }

  def jdbcBatch(df: DataFrame, url: String, driver: String, query: String, user: Option[String], password: Option[String], params: Map[String, String]): DataFrame = {      
    import df.sparkSession.implicits._

    df.foreachPartition((partition: Iterator[Row]) => {
      partition.map(f => "foo")
    })

  }

  def jdbcQuery(df: DataFrame, url: String, driver: String, query: String, user: Option[String], password: Option[String], params: Map[String, String]): DataFrame = {
    df
  }
}

private[almaren] trait JDBCConnector extends Core {
  def jdbcBatch(url: String, driver: String, query: String, user: Option[String] = None, password: Option[String] = None, params: Map[String, String] = Map()): Option[Tree] =
    MainJDBC(
      url,
      driver,
      query,
      user,
      password,
      params
    )
}

object JDBC {
  implicit class JDBCImplicit(val container: Option[Tree]) extends JDBCConnector
}
