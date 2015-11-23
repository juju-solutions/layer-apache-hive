## Overview

Apache Hive is a data warehouse infrastructure built on top of Hadoop that
supports data summarization, query, and analysis. Hive provides an SQL-like
language called HiveQL that transparently converts queries to MapReduce for
execution on large datasets stored in Hadoop's HDFS. Learn more at
[hive.apache.org](http://hive.apache.org).

This charm provides the Hive command line interface and the HiveServer2 service.


## Usage
This charm leverages our pluggable Hadoop model with the `hadoop-plugin`
interface. This means that you will need to deploy a base Apache Hadoop cluster
to run Hive. The suggested deployment method is to use the
[apache-analytics-sql](https://jujucharms.com/apache-analytics-sql/)
bundle. This will deploy the Apache Hadoop platform with a single Apache Hive
unit that communicates with the cluster by relating to the
`apache-hadoop-plugin` subordinate charm:

    juju quickstart apache-analytics-sql

Alternatively, you may manually deploy the recommended environment as follows:

    juju deploy apache-hadoop-hdfs-master hdfs-master
    juju deploy apache-hadoop-yarn-master yarn-master
    juju deploy apache-hadoop-compute-slave compute-slave
    juju deploy apache-hadoop-plugin plugin
    juju deploy apache-hive hive

    juju deploy mysql
    juju set mysql binlog-format=ROW

    juju add-relation yarn-master hdfs-master
    juju add-relation compute-slave yarn-master
    juju add-relation compute-slave hdfs-master
    juju add-relation plugin yarn-master
    juju add-relation plugin hdfs-master
    juju add-relation hive plugin
    juju add-relation hive mysql

Please note the special configuration for the mysql charm above; MySQL must be
using row-based logging for information to be replicated to Hadoop.


## Testing the deployment

### Smoke test Hive
From the Hive unit, start the Hive console as the `hive` user:

    juju ssh hive/0
    sudo su hive -c hive

From the Hive console, verify sample commands execute successfully:

    show databases;
    create table test(col1 int, col2 string);
    show tables;
    quit;

Exit from the Hive unit:

    exit

### Smoke test HiveServer2
From the Hive unit, start the Beeline console as the `hive` user:

    juju ssh hive/0
    sudo su hive -c beeline

From the Beeline console, connect to HiveServer2 and verify sample commands
execute successfully:

    !connect jdbc:hive2://localhost:10000 hive password org.apache.hive.jdbc.HiveDriver
    show databases;
    create table test2(a int, b string);
    show tables;
    !quit

Exit from the Hive unit:

    exit


## Contact Information

- <bigdata@lists.ubuntu.com>


## Help

- [Juju mailing list](https://lists.ubuntu.com/mailman/listinfo/juju)
- [Juju community](https://jujucharms.com/community)
