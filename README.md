## Overview

Apache Hive is a data warehouse infrastructure built on top of Hadoop that
supports data summarization, query, and analysis. Hive provides an SQL-like
language called HiveQL that transparently converts queries to MapReduce for
execution on large datasets stored in Hadoop's HDFS. Learn more at
[hive.apache.org](http://hive.apache.org).

This charm provides the Hive command line interface and the HiveServer2 service.


## Usage
This charm is uses the Hadoob base layer and the HDFS interface to pull its dependencies
and act as a client to a Hadoop namenode:

You may manually deploy the recommended environment as follows:

    juju deploy apache-hadoop-datanode datanode
    juju deploy apache-hadoop-namenode namenode
    juju deploy apache-hadoop-nodemanager nodemgr
    juju deploy apache-hadoop-resourcemanager resourcemgr

    juju add-relation namenode datanode
    juju add-relation resourcemgr nodemgr
    juju add-relation resourcemgr namenode

    juju deploy mysql
    juju set mysql binlog-format=ROW


Deploy Hive charm:

    juju deploy apache-hive hive
    juju add-relation hive namenode
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
