# JDBC Connector

[![Build Status](https://travis-ci.com/modakanalytics/jdbc.almaren.svg?token=TEB3zRDqVUuChez9334q&branch=master)](https://travis-ci.com/modakanalytics/jdbc.almaren)

JDBC Connector allow you to execute any SQL statement using Apache Spark.

```
libraryDependencies += "com.github.music-of-the-ainur" %% "jdbc-almaren" % "0.0.1-2.4"
```

```
spark-shell --master "local[*]" --packages "com.github.music-of-the-ainur:almaren-framework_2.11:0.9.0-2.4,com.github.music-of-the-ainur:jdbc-almaren_2.11:0.1.3-2.4"
```

## Example

```scala
import com.github.music.of.the.ainur.almaren.Almaren
import com.github.music.of.the.ainur.almaren.builder.Core.Implicit
import com.github.music.of.the.ainur.almaren.jdbc.JDBC.JDBCImplicit

import spark.implicits._

```

```

## Parameters

| Parameter      | Description                                                                                                             | Type                                                               |
|----------------|-------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------|

## Special Columns

### Input:

| Parameters   | Mandatory | Description                                                                        |
|--------------|-----------|------------------------------------------------------------------------------------|


### Output:

| Parameters           | Description                                        |
|----------------------|----------------------------------------------------|
