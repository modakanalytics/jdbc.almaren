package com.github.music.of.the.ainur.almaren.jdbc

import java.util.concurrent.Executors

import com.github.music.of.the.ainur.almaren.Tree
import com.github.music.of.the.ainur.almaren.builder.Core
import com.github.music.of.the.ainur.almaren.state.core.Main
import org.apache.spark.sql.{DataFrame, Row}
import scalikejdbc._

import scala.util.{Failure, Success, Try}

final case class JDBCResponse(
  `__ID__`:String,
  `__URL__`:String,
  `__DRIVER__`:String,
  `__QUERY__`:String,
  `__BATCH_SIZE__`:Int,
  `__ELAPSED_TIME__`:Long,
  `__ERROR__`:Option[String] = None
)

private[almaren] case class MainJDBCBatch(url: String, driver: String, query: String, batchSize: Int, user: Option[String], password: Option[String], params: Map[String, String]) extends Main {

  lazy val settings = ConnectionPoolSettings(
    initialSize = 1,
    maxSize =  params.getOrElse("maxSize",10).toString.toInt,
    connectionTimeoutMillis = params.getOrElse("connectionTimeoutMillis",3000).toString.toInt
  )

  private object Alias {
    val IdCol = "__ID__"
  }

  override def core(df: DataFrame): DataFrame = {
    logger.info(s"url:{$url}, driver:{$driver}, query:{$query}, batchSize:{$batchSize}, user:{$user}, params:{$params}")
    jdbcBatch(df)
  }

  def jdbcBatch(df: DataFrame): DataFrame = {
    import df.sparkSession.implicits._

    val result = df.mapPartitions((partition: Iterator[Row]) => {
      Class.forName(driver)
      ConnectionPool.singleton(url, user.getOrElse(""), password.getOrElse(""), settings)
      partition.grouped(batchSize).flatMap(rows => batchQuery(rows))
    })
    result.toDF
  }

  private def batchQuery(rows:Seq[Row]): Seq[JDBCResponse] = {
    val batchParams: Seq[Seq[Any]] = rows.map(row => {
      (0 to (row.size - 1)).map(index => row.get(index)).toSeq
    }).toSeq
    val startTime = System.currentTimeMillis()
    DB localTx { implicit session =>
      // The "tail" is to remove the first column __ID__ from the insert.
      Try { sql"${SQLSyntax.createUnsafely(query)}".batch(batchParams.map(_.tail): _*).apply() } match {
        case Success(data) =>  batchParams.map(row =>           
          JDBCResponse(
            `__ID__` = row.head.toString(),
            `__URL__` = url,
            `__DRIVER__` = driver,
            `__QUERY__` = query,
            `__BATCH_SIZE__` = batchSize,
            `__ELAPSED_TIME__` = System.currentTimeMillis() - startTime)
        )
        case Failure(error) => {
          logger.error("Almaren jdbcBatch error", error)
          batchParams.map(row =>           
            JDBCResponse(
              `__ID__` = row.head.toString(),
              `__URL__` = url,
              `__DRIVER__` = driver,
              `__QUERY__` = query,
              `__BATCH_SIZE__` = batchSize,
              `__ELAPSED_TIME__` = System.currentTimeMillis() - startTime,
              `__ERROR__` = Some(s"${error.getMessage}, ${error.getLocalizedMessage()}"))
          )
        }
      }
    }
  }
}

private[almaren] case class MainJDBCConnector(url: String, driver: String, query: String, user: Option[String], password: Option[String], params: Map[String, String]) extends Main {

  lazy val settings = ConnectionPoolSettings(
    initialSize = 1,
    maxSize =  params.getOrElse("maxSize",10).toString.toInt,
    connectionTimeoutMillis = params.getOrElse("connectionTimeoutMillis",3000).toString.toInt
  )

  Class.forName(driver)
  ConnectionPool.singleton(url, user.getOrElse(""), password.getOrElse(""), settings)

  override def core(df: DataFrame): DataFrame = {
    logger.info(s"url:{$url}, driver:{$driver}, query:{$query}, user:{$user}, params:{$params}")
    import df.sparkSession.implicits._

    List(jdbcExecute(query)).toDF
  }

  def jdbcExecute(query:String): Int = {
    DB autoCommit { implicit session =>
      sql"${SQLSyntax.createUnsafely(query)}".executeUpdate().apply()
    }
  }
}

private[almaren] trait JDBCBatch extends Core {
  def jdbcBatch(url: String, driver: String, query: String, batchSize: Int = 1000, user: Option[String] = None, password: Option[String] = None, params: Map[String, String] = Map()): Option[Tree] =
    MainJDBCBatch(
      url,
      driver,
      query,
      batchSize,
      user,
      password,
      params
    )
}

private[almaren] trait JDBCConnector extends Core {
  def jdbcQuery(url: String, driver: String, query: String, user: Option[String] = None, password: Option[String] = None, params: Map[String, String] = Map()): Option[Tree] =
    MainJDBCConnector(
      url,
      driver,
      query,
      user,
      password,
      params
    )
}

object JDBC {
  implicit class JDBCBatchImplicit(val container: Option[Tree]) extends JDBCBatch
  implicit class JDBCImplicit(val container: Option[Tree]) extends JDBCConnector
}
