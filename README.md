# JDBC Connector

[![Build Status](https://travis-ci.com/modakanalytics/jdbc.almaren.svg?token=TEB3zRDqVUuChez9334q&branch=master)](https://travis-ci.com/modakanalytics/jdbc.almaren)

JDBC Connector allow you to execute any SQL statement using Apache Spark.

```
libraryDependencies += "com.github.music-of-the-ainur" %% "jdbc-almaren" % "0.0.1-2.4"
```

```
spark-shell --master "local[*]" --packages "com.github.music-of-the-ainur:almaren-framework_2.11:0.9.0-2.4,com.github.music-of-the-ainur:jdbc-almaren_2.11:0.0.1-2.4"
```

## Example

```scala
import com.github.music.of.the.ainur.almaren.Almaren
import com.github.music.of.the.ainur.almaren.builder.Core.Implicit
import com.github.music.of.the.ainur.almaren.jdbc.JDBC.JDBCImplicit

import spark.implicits._
val almaren = Almaren("jdbc-almaren")

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
```
## Parameters

| Parameter | Description                                                            | Type               |
|-----------|------------------------------------------------------------------------|--------------------|
| url       | The JDBC URL to connect to                                             | String             |
| driver    | The class name of the JDBC driver to use to connect to this URL.       | String             |
| query     | Query to be executed                                                   | String             |
| batchSize | How many records a single thread will process                          | Int                |
| user      | Database user                                                          | Option[String]     |
| password  | Database password                                                      | Option[String]     |
| params    | Other extra parameters  like connectionTimeout  etc ..can be specified | Map[String,String] |


## Special Columns

### Input:

| Parameters | Mandatory | Description                                                                        |
|------------|-----------|------------------------------------------------------------------------------------|
| \_\_ID\_\_ | Yes       | This field will be in response of jdbc.almaren component, it's useful to join data |

### Output:

| Parameters           | Description                                                      |
|----------------------|------------------------------------------------------------------|
| \_\_ID\_\_           | Custom ID , This field will be useful to join data               |
| \_\_URL\_\_          | The JDBC URL used to connect to                                  |
| \_\_DRIVER\_\_       | The class name of the JDBC driver to used to connect to this URL |
| \_\_QUERY\_\_        | Query executed                                                   |
| \_\_BATCHSIZE\_\_    | How many records a single thread will process                    |
| \_\_ELAPSED_TIME\_\_ | Query Execution time                                             |
| \_\_ERROR\_\_        | Error message if query execution fails                           |
