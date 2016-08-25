---
layout: global
displayTitle: Integration with Cloud Infrastructures
title: Integration with Cloud Infrastructures
description: Introduction to cloud storage support in Apache Spark SPARK_VERSION_SHORT
---
<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at
  
   http://www.apache.org/licenses/LICENSE-2.0
  
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

* This will become a table of contents (this text will be scraped).
{:toc}

## Introduction

Apache Spark can use cloud object stores as a source or destination of data. It does so
through filesystem connectors implemented in Apache Hadoop or provided by third-parties.
Provided the relevant libraries are on the classpath, a file can be referenced simply
via its URL

```scala
sparkContext.textFile("s3a://landsat-pds/scene_list.gz").count()
```

Similarly, an RDD can be saved to an object store via `saveAsTextFile()`


```scala
val numbers = sparkContext.parallelize(1 to 1000)
// save to Amazon S3 (or compatible implementation)
numbers.saveAsTextFile("s3a://testbucket/counts")
// save to an OpenStack Swift implementation
numbers.saveAsTextFile("swift://testbucket.rackspace/counts")
// Save to Azure Object store
numbers.saveAsTextFile("wasb://testbucket@example.blob.core.windows.net/counts")
```

### Example: DataFrames

DataFrames

```scala
val spark = SparkSession
    .builder
    .appName("S3DataFrames")
    .config(sparkConf)
    .getOrCreate()
import spark.implicits._
val numRows = 1000
val sourceData = spark.range(0, numRows).select($"id".as("l"), $"id".cast(StringType).as("s"))
val dest = "wasb://yourcontainer@youraccount.blob.core.windows.net/dataframes"
val orcFile = dest + "/data.orc"
// write the data
sourceData.write.format("orc").save(orcFile)
// read it back
val orcData = spark.read.format("orc").load(orcFile)
// save it to parquet
val parquetFile = dest + "/data.parquet"
orcData.write.format("parquet").save(parquetFile)
spark.stop()
```

### Example: Spark Streaming and Cloud Storage

Spark Streaming can monitor files added to object stores, by
creating a `FileInputDStream` DStream monitoring a path under a bucket.

```scala
val sparkConf = new SparkConf()
val ssc = new StreamingContext(sparkConf, Milliseconds(5000))
try {
  val lines = ssc.textFileStream("s3a://bucket/incoming")
  val matches = lines.filter(_.endsWith("3"))
  matches.print()
  ssc.start()
  ssc.awaitTermination()
} finally {
  ssc.stop(true)
}
```

Be advised that the time to scan for new files is proportional to the number of files
under the path —not the number of *new* files, and that it can become a slow operation.

## Object stores and their library dependencies

The different object stores supported by Spark depend on specific Hadoop versions,
and require specific Hadoop JARs and dependent Java libraries on the classpath.

### Amazon S3 with s3a://

The "S3A" filesystem is a connector with Amazon S3, initially implemented in Hadoop 2.6, and
considered ready for production use in Hadoop 2.7.

The implementation is `hadoop-aws`, which is included in `$SPARK_HOME/jars`  when spark
is built against Hadoop 2.7 or later.

Dependencies: `amazon-aws-sdk` JAR (Hadoop 2.6 and 2.7); `amazon-s3-sdk` and `amazon-core-sdk`
in Hadoop 2.8. *Warning*: The Amazon JARs have proven very brittle —the version of the Amazon
libraries *must* match that which the Hadoop binaries were built against.

### Amazon S3 with s3n://

The "S3N" filesystem connector is a long-standing connector shipping with all versions of Hadoop 2.
It uses the `jets3t` library to talk to HDFS; this must be on the classpath.

S3N is essentially unmaintained by the Hadoop team and, on Hadoop 2.7+ is deprecated in
favor of S3A. Only critical security issues are being fixed on S3N.

### Microsoft Azure with wasb://

The `wasb` filesystem connector is implemented in  the`hadoop-azure` JAR.
It needs the `azure-storage` JAR on the classpath.

### Openstack Swift

The `swift` filesystem connector is implemented in `hadoop-openstack`.

## Cloud object stores are not filesystems

Object stores are not filesystems: they are not a hierarchical tree of directories and files.

The Hadoop filesystem APIs offer a filesystem API to the object stores, but underneath
they are still object stores, [and the difference is significant](http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/filesystem/introduction.html)

While object stores can be used as the source and destination of data, they cannot be
used as a direct replacement for a cluster-wide filesystem, such as HDFS.
This is important to know, as the fact they are easy to work with can be misleading.

### Directory operations may be slow and not atomic

Directory rename and delete may be performed as a series of operations on the client. Specifically,
`delete(path, recursive=true)` may be implemented as "list the objects, delete them singly or in batches".
`rename(source, dest)` may be implemented as "copy all the objects" followed by the delete operation.

1. They may fail part way through, leaving the status of the filesystem "undefined".
1. The time to delete may be `O(files)`
1. The time to rename may be `O(data)`. If the rename is done on the client, the time to rename
each file will depend upon the bandwidth between client and the filesystem. The further away the client
is, the longer the rename will take.
1. Recursive directory listing can be very slow. This can slow down some parts of job submission
and execution.

Because of these behaviours, committing of work by renaming directories is neither efficient nor
reliable. There is a special output committer for Parquet,
the `org.apache.spark.sql.execution.datasources.parquet.DirectParquetOutputCommitter`
which bypasses the rename phase.

*Critical* speculative execution does not work against object
stores which do not support atomic directory renames. Your output may get
corrupted.

*Warning* even non-speculative execution is at risk of leaving the output of a job in an inconsistent
state if a "Direct" output committer is used and executors fail.

### Data may not be written until the output stream's `close()` operation.

Data to be written to the object store is usually buffered to a local file or stored in memory,
until one of: there is enough data to create a partition in a multi-partitioned upload (where enabled),
or when the output stream's `close()` operation is done.

- If the process writing the data fails, no data at all may have been saved to the object store.
- Data may be visible in the object store until the entire output stream is complete
- There may not be an entry in the object store for the file (even a 0 byte one) until
that stage.

### An object store may display eventual consistency

Object stores are often *Eventually Consistent*. This can surface, in particular:-

- When listing "a directory"; newly created files may not yet be visible, deleted ones still present.
- After updating an object: opening and reading the object may still return the previous data.
- After deleting an obect: opening it may succeed, returning the data.
- While reading an object, if it is updated or deleted during the process.

For many years, Amazon US East S3 lacked create consistency: attempting to open a newly created object
could return a 404 response, which Hadoop maps to a `FileNotFoundException`. This was fixed in August 2015
—see [S3 Consistency Model](http://docs.aws.amazon.com/AmazonS3/latest/dev/Introduction.html#ConsistencyModel)
for the full details.

### Read operations may be significantly slower than normal filesystem operations.

Object stores usually implement their APIs as HTTP operations; clients make HTTP(S) requests
and block for responses. Each of these calls can be expensive. For maximum performance

1. Try to list filesystem paths in bulk.
1. Know that `FileSystem.getFileStatus()` is expensive: cache the results rather than repeat
the call (or wrapper methods such as `FileSystem.exists(), isDirectory() or isFile()`).
1. Try to forward `seek()` through a file, rather than backwards.
1. Avoid renaming files: This is slow and, if it fails, may fail leave the destination in a mess.
1. Use the local filesystem as the destination of output which you intend to reload in follow-on work.
Retain the object store as the final destination of persistent output, not as a replacement for
HDFS.


## Testing Spark's cloud support

The `spark-cloud` module contains tests which can run against the object stores. These verify
functionality integration and performance.

### Example Configuration for testing cloud data


The test runs need a configuration file to declare the (secret) bindings to the cloud infrastructure.
The configuration used is the Hadoop XML format, because it allows XInclude importing of
secrets kept out of any source tree.

The secret properties are defined using the Hadoop configuration option names, such as
`fs.s3a.access.key` and `fs.s3a.secret.key`

The file must be declared to the maven test run in the property `cloud.test.configuration.file`,
which can be done in the command line

```
mvn test  --pl cloud -Dcloud.test.configuration.file=../cloud.xml
```

*Important*: keep all credentials out of SCM-managed repositories. Even if `.gitignore`
or equivalent is used to exclude the file, they may unintenally get bundled and released
with an application. It is safest to keep the `cloud.xml` files out of the tree, 
and keep the authentication secrets themselves in a single location for all applications
tested.

Here is an example XML file `/home/developer/aws/cloud.xml` for running the S3A and Azure tests,
referencing the secret credentials kept in the file `/home/hadoop/aws/auth-keys.xml`.

```xml
<configuration>
  <include xmlns="http://www.w3.org/2001/XInclude"
    href="file:///home/developer/aws/auth-keys.xml"/>

  <property>
    <name>s3a.tests.enabled</name>
    <value>true</value>
    <description>Flag to enable S3A tests</description>
  </property>

  <property>
    <name>s3a.test.uri</name>
    <value>s3a://testplan1</value>
    <description>S3A path to a bucket which the test runs are free to write, read and delete
    data.</description>
  </property>

  <property>
    <name>azure.tests.enabled</name>
    <value>true</value>
  </property>

  <property>
    <name>azure.test.uri</name>
    <value>wasb://MYCONTAINER@TESTACCOUNT.blob.core.windows.net</value>
  </property>

</configuration>
```

The configuration uses XInclude to pull in the secret credentials for the account
from the user's `/home/developer/.ssh/auth-keys.xml` file:

```xml
<configuration>
  <property>
    <name>fs.s3a.access.key</name>
    <value>USERKEY</value>
  </property>
  <property>
    <name>fs.s3a.secret.key</name>
    <value>SECRET_AWS_KEY</value>
  </property>
  <property>
    <name>fs.azure.account.key.TESTACCOUNT.blob.core.windows.net</name>
    <value>SECRET_AZURE_KEY</value>
  </property>
</configuration>
```

Splitting the secret values out of the other XML files allows for the other files to
be managed via SCM and/or shared, with reduced risk.

Note that the configuration file is used to define the entire Hadoop configuration used
within the Spark Context created; all options for the specific test filesystems may be
defined, such as endpoints and timeouts.

### S3A Options

<table class="table">
  <tr><th style="width:21%">Option</th><th>Meaning</th><th>Default</th></tr>
  <tr>
    <td><code>s3a.tests.enabled</code></td>
    <td>
    Execute tests using the S3A filesystem.
    </td>
    <td><code>false</code></td>
  </tr>
  <tr>
    <td><code>s3a.test.uri</code></td>
    <td>
    URI for S3A tests. Rquired if S3A tests are enabled.
    </td>
    <td><code></code></td>
  </tr>
  <tr>
    <td><code>s3a.test.csvfile.path</code></td>
    <td>
    Path to a (possibly encrypted) CSV file used in linecount tests.
    </td>
    <td><code></code>s3a://landsat-pds/scene_list.gz</td>
  </tr>
  <tr>
    <td><code>s3a.test.csvfile.endpoint</code></td>
    <td>
    Endpoint URI for CSV test file. This allows a different S3 instance
    to be set for tests reading or writing data than against public CSV
    source files.
    Example: <code>s3.amazonaws.com</code>
    </td>
    <td><code>s3.amazonaws.com</code></td>
  </tr>
</table>

When testing against Amazon S3, their [public datasets](https://aws.amazon.com/public-data-sets/)
are used. 

The gzipped CSV file `s3a://landsat-pds/scene_list.gz`` is used for testing line input and file IO; the default
is a 20+ MB file hosted by Amazon. This file is public and free for anyone to
access, making it convenient and cost effective. 

The size and number of lines in this file increases over time; 
the current size of the file can be measured through `curl`:

```bash
curl -I -X HEAD http://landsat-pds.s3.amazonaws.com/scene_list.gz
```

When testing against non-AWS infrastructure, an alternate file may be specified
in the option `s3a.test.csvfile.path`; with its endpoint set to that of the
S3 endpoint


```xml
  <property>
    <name>s3a.test.csvfile.path</name>
    <value>s3a://testdata/landsat.gz</value>
  </property>
  
  <property>
    <name>fs.s3a.endpoint</name>
    <value>s3server.example.org</value>
  </property>

  <property>
    <name>s3a.test.csvfile.endpoint</name>
    <value>${fs.s3a.endpoint}</value>
  </property>

```


When testing against an S3 instance which only supports the AWS V4 Authentication
API, such as Frankfurt and Seoul, the `fs.s3a.endpoint` property must be set to that of
the specific location. Because the public landsat dataset is hosted in AWS US-East, it must retain
the original S3 endpoint. This is done by default, though it can also be set explicitly:


```xml
<property>
  <name>fs.s3a.endpoint</name>
  <value>s3.eu-central-1.amazonaws.com</value>
</property>

<property>
  <name>s3a.test.csvfile.endpoint</name>
  <value>s3.amazonaws.com</value>
</property>
```

Finally, the CSV file tests can be skipped entirely by declaring the URL to be""


```xml
<property>
  <name>s3a.test.csvfile.path</name>
  <value></value>
</property>
```
## Azure Test Options


<table class="table">
  <tr><th style="width:21%">Option</th><th>Meaning</th><th>Default</th></tr>
  <tr>
    <td><code>azure.tests.enabled</code></td>
    <td>
    Execute tests using the Azure WASB filesystem
    </td>
    <td><code>false</code></td>
  </tr>
  <tr>
    <td><code>azure.test.uri</code></td>
    <td>
    URI for Azure WASB tests. Required if Azure tests are enabled.
    </td>
    <td><code></code></td>
  </tr>
</table>


## Running a single test case

Each cloud test takes time; it is convenient to be able to work on a single test case at a time
when implementing or debugging a test.

Tests in a cloud suite must be conditional on the specific filesystem being available; every
test suite must implement a method `enabled: Boolean` to determine this. The tests are then
registered as "conditional tests", with a key, a detailed description (this is included in logs),
and the actual function to execute.

For example, here is the test `NewHadoopAPI`.

```scala

  ctest("NewHadoopAPI", 
    "Use SparkContext.saveAsNewAPIHadoopFile() to save data to a file") {
    sc = new SparkContext("local", "test", newSparkConf())
    val numbers = sc.parallelize(1 to testEntryCount)
    val example1 = new Path(TestDir, "example1")
    saveAsTextFile(numbers, example1, sc.hadoopConfiguration)
  }
```

It can be executed as part of the suite `S3aIOSuite`, by setting the `suites` maven property to the classname
of the test suite:

```
mvn test --pl cloud -Phadoop-2.7 -Dcloud.test.configuration.file=/home/developer/aws/cloud.xml -Dsuites=org.apache.spark.cloud.s3.S3aIOSuite
```

The specific test can be explicitly run by including the key in the `suites` property
after the suite name

```
mvn test --pl cloud -Phadoop-2.7 -Dcloud.test.configuration.file=/home/developer/aws/cloud.xml `-Dsuites=org.apache.spark.cloud.s3.S3aIOSuite NewHadoopAPI`
```

This will run all tests in the `S3aIOSuite` suite whose name contains the string `NewHadoopAPI`;
here just one test.

To run all tests of a specific infrastructure, use the `wildcardSuites` property to list the package
under which all test suites should be executed.

```
mvn test --pl cloud -Phadoop-2.7 -Dcloud.test.configuration.file=/home/developer/aws/cloud.xml `-DwildcardSuites=org.apache.spark.cloud.s3`
```

Note that an absolute path is used to refer to the test configuration file in these examples.
If a relative path is supplied, it should be relative to the project base.

## Best practices for adding a new test

1. Use `ctest()` to define a test case conditional on the suite being enabled.
1. Give it a unique name which can be used to explicitly execute it from the build via the `suite` property.
1. Give it a name useful in test reports/bug reports.
1. Give it a meaningful description for logs and test reports..
1. Test against multiple infrastructure instances.


## Best practices for adding a new test suite

1. Extend `CloudSuite`
1. Have an `after {}` clause which cleans up all object stores —this keeps costs down.
1. Do not assume that any test has exclusive access to any part of an object store other
than the specific test directory. This is critical to support parallel test execution.
1. Share setup costs across test cases, especially for slow directory/file setup operations.
1. If extra conditions are needed before a test suite can be executed, override the `enabled` method
to probe for the extra conditions being met.

## Keeping Test costs low

Object stores incur charges for storage and for IO out of the datacenter where the data is stored.

The tests try to keep costs down by not working with large amounts of data, and by deleting
all data on teardown. If a test run is aborted, data may be retained on the test filesystem.
While the charges should only be a small amount, period purges of the bucket will keep costs down.

Rerunning the tests to completion again should achieve this.

The large dataset tests read in public data, so storage and bandwidth costs
are incurred by Amazon and other cloud storage providers themselves.

### Keeping credentials safe

It is critical that the credentials used to access object stores are kept secret. Not only can
they be abused to run up compute charges, they can be used to read and alter private data.

1. Keep the XML Configuration file with any secrets in a secure part of your filesystem.
1. When using Hadoop 2.8+, consider using Hadoop credential files to store secrets, referencing
these files in the relevant id/secret properties of the XML configuration file.
1. Do not execute these tests as part of automated CI/Jenkins builds, unless the secrets are
not senstitive -for example, they refer to in-house (test) object stores, authentication is
done via IAM EC2 VM credentials, or the credentials are short-lived AWS STS-issued credentials
with a lifespan of minutes and access only to transient test buckets.
