# JDBC Connector

[![Build Status](https://travis-ci.com/modakanalytics/jdbc.almaren.svg?token=TEB3zRDqVUuChez9334q&branch=master)](https://travis-ci.com/modakanalytics/jdbc.almaren)

JDBC Connector allow you to execute any SQL statement using Apache Spark.

```
libraryDependencies += "com.github.music-of-the-ainur" %% "jdbc-almaren" % "0.0.4-2.4"
```

```
spark-shell --master "local[*]" --packages "com.github.music-of-the-ainur:almaren-framework_2.12:0.9.3-$SPARK_VERSION,com.github.music-of-the-ainur:jdbc-almaren_2.12:0.0.4-$SPARK_VERSION"
```
## JDBC Batch

### Example

```scala
import com.github.music.of.the.ainur.almaren.Almaren
import com.github.music.of.the.ainur.almaren.builder.Core.Implicit
import com.github.music.of.the.ainur.almaren.jdbc.JDBC.JDBCImplicit

import spark.implicits._

 val almaren = Almaren("jdbc-almaren")

 val updateSourceDf = Seq(
    ("John", "Jones"),
    ("David", "Smith"),
    ("Michael", "Lee"),
    ("Chris", "Johnson"),
    ("Mike", "Brown")
  ).toDF("first_name", "last_name")

  val updateQuery = "UPDATE person_info set first_name = ? where last_name = ?"

  almaren.builder
    .sourceDataFrame(updateSourceDf)
    .sql("select monotonically_increasing_id() as __ID__,first_name,last_name from __TABLE__")
    .jdbcBatch("jdbc:postgresql://localhost:5432/almaren", "org.postgresql.Driver", updateQuery, 1000, Some("postgres"), Some("postgres"),Map("connectionTimeoutMillis" -> "3000","maxSize"->"10"))
    .batch
    .count
```

### Parameters

| Parameter | Description                                                                 | Type               |
|-----------|-----------------------------------------------------------------------------|--------------------|
| url       | The JDBC URL to connect to                                                  | String             |
| driver    | The class name of the JDBC driver to use to connect to this URL.            | String             |
| query     | Query to be executed                                                        | String             |
| batchSize | Number of records that will be send to the database in a single transaction | Int                |
| user      | Database user                                                               | Option[String]     |
| password  | Database password                                                           | Option[String]     |
| params    | Other extra parameters  like connectionTimeout  etc ..can be specified      | Map[String,String] |


### Special Columns

#### Input:

| Parameters | Mandatory                       | Description                                                                        |
|------------|---------------------------------|------------------------------------------------------------------------------------|
| \_\_ID\_\_ | Yes(Should be the first column) | This field will be in response of jdbc.almaren component, it's useful to join data |

#### Output:


| Parameters           | Description                                                                 |
|----------------------|-----------------------------------------------------------------------------|
| \_\_ID\_\_           | Custom ID , This field will be useful to join data                          |
| \_\_URL\_\_          | The JDBC URL used to connect to                                             |
| \_\_DRIVER\_\_       | The class name of the JDBC driver to used to connect to this URL            |
| \_\_QUERY\_\_        | Query executed                                                              |
| \_\_BATCHSIZE\_\_    | Number of records that will be send to the database in a single transaction |
| \_\_ELAPSED_TIME\_\_ | Query Execution time                                                        |
| \_\_ERROR\_\_        | Error message if query execution fails                                      |


## JDBC Query 

### Example 

```scala
import com.github.music.of.the.ainur.almaren.Almaren
import com.github.music.of.the.ainur.almaren.builder.Core.Implicit
import com.github.music.of.the.ainur.almaren.jdbc.JDBC.JDBCImplicit

import spark.implicits._

 val almaren = Almaren("jdbc-almaren")

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

   almaren.builder
        .jdbcQuery("jdbc:postgresql://localhost:5432/almaren", "org.postgresql.Driver", mergeQuery, Some("postgres"), Some("postgres"),Map("connectionTimeoutMillis" -> "3000","maxSize"->"10"))
        .batch
```
### Parameters

| Parameter | Description                                                                 | Type               |
|-----------|-----------------------------------------------------------------------------|--------------------|
| url       | The JDBC URL to connect to                                                  | String             |
| driver    | The class name of the JDBC driver to use to connect to this URL.            | String             |
| query     | Query to be executed                                                        | String             |
| user      | Database user                                                               | Option[String]     |
| password  | Database password                                                           | Option[String]     |
| params    | Extra parameters  like connectionTimeout  etc ..can be specified            | Map[String,String] |

### Params 

| Parameter | Description                                                                 |
|-----------|-----------------------------------------------------------------------------|
| connectionTimeoutMillis       |    The time for which the connection gets timed out                          | 
| maxSize       | Maximum number of connections available                             |        
