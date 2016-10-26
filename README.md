## Druid Hadoop InputFormat

This is a Hadoop InputFormat that can be used to load Druid data from deep storage.

### Installation

To install this library, run `mvn install`. You can then include it in projects with Maven by using the dependency:

```xml
<dependency>
  <groupId>io.imply</groupId>
  <artifactId>druid-hadoop-inputformat</artifactId>
  <version>0.1-SNAPSHOT</version>
</dependency>
```

### Example

Here's an example of creating an RDD in Spark:

```java
final JobConf jobConf = new JobConf();
final String coordinatorHost = "localhost:8081";
final String dataSource = "wikiticker";
final List<Interval> intervals = null; // null to include all time
final DimFilter filter = null; // null to include all rows
final List<String> columns = null; // null to include all columns

DruidInputFormat.setInputs(
    jobConf,
    coordinatorHost,
    dataSource,
    intervals,
    filter,
    columns
);

final JavaPairRDD<NullWritable, InputRow> rdd = jsc.newAPIHadoopRDD(
    jobConf,
    DruidInputFormat.class,
    NullWritable.class,
    InputRow.class
);
```
