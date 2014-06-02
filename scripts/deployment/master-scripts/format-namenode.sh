#!/bin/bash
export HADOOP_PREFIX=/opt/hop/hadoop-2.0.4-alpha
export JAVA_HOME=/usr/lib/jvm/java-6-oracle
$HADOOP_PREFIX/bin/hdfs namenode -format
