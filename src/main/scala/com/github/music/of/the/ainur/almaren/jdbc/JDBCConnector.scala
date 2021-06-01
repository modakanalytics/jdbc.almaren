package com.github.music.of.the.ainur.almaren.jdbc

import java.util.concurrent.Executors

import com.github.music.of.the.ainur.almaren.Tree
import com.github.music.of.the.ainur.almaren.builder.Core
import com.github.music.of.the.ainur.almaren.state.core.Main
import org.apache.spark.sql.{DataFrame, Row}
import scalikejdbc._

import scala.util.{Failure, Success, Try}

final case class JDBCResponse(
  `__ERROR__`:Option[String] = None,
  `__BATCH_SIZE__`:Int,
  `__URL__`:String,
  `__DRIVER__`:String,
  `__ELAPSED_TIME__`:Long
)

private[almaren] case class MainJDBC(url: String, driver: String, query: String, batchSize: Int, user: Option[String], password: Option[String], params: Map[String, String]) extends Main {

  override def core(df: DataFrame): DataFrame = {
    logger.info(s"url:{$url}, driver:{$driver}, query:{$query}, batchSize:{$batchSize}, user:{$user}, params:{$params}")
    df
  }

  def jdbcBatch(df: DataFrame): DataFrame = {
    import df.sparkSession.implicits._

    val result = df.mapPartitions((partition: Iterator[Row]) => {

      Class.forName(driver)
      ConnectionPool.singleton(url, user.getOrElse(""), password.getOrElse(""))

      partition.grouped(batchSize).map(rows => {
        val batchParams: Seq[Seq[Any]] = rows.map(row => {
          (0 to row.size).map(index => row.get(index - 1)).toSeq
        }).toSeq
        val startTime = System.currentTimeMillis()
        DB localTx { implicit session =>
          Try { sql"${SQLSyntax.createUnsafely(query)}".batch(batchParams: _*).apply() } match {
            case Success(data) => JDBCResponse(
              `__BATCH_SIZE__` = batchSize,
              `__URL__` = url,
              `__DRIVER__` = driver,
              `__ELAPSED_TIME__` = System.currentTimeMillis() - startTime)
            case Failure(error) => {
              logger.error("Almaren jdbcBatch error", error)
              JDBCResponse(
                `__ERROR__` = Some(error.getMessage),
                `__BATCH_SIZE__` = batchSize,
                `__URL__` = url,
                `__DRIVER__` = driver,
                `__ELAPSED_TIME__` = System.currentTimeMillis() - startTime)
            }
          }
        }
      })
    })
    result.toDF
  }

  def jdbcQuery(df: DataFrame): DataFrame = {
    df
  }
}

private[almaren] trait JDBCConnector extends Core {
  def jdbcBatch(url: String, driver: String, query: String, batchSize: Int = 1000, user: Option[String] = None, password: Option[String] = None, params: Map[String, String] = Map()): Option[Tree] =
    MainJDBC(
      url,
      driver,
      query,
      batchSize,
      user,
      password,
      params
    )
}

object JDBC {
  implicit class JDBCImplicit(val container: Option[Tree]) extends JDBCConnector
}
