#!/bin/bash
export HADOOP_PREFIX=/opt/hop/hadoop-2.0.4-alpha

$HADOOP_PREFIX/sbin/yarn-daemon.sh start resourcemanager
$HADOOP_PREFIX/sbin/hadoop-daemon.sh start namenode
$HADOOP_PREFIX/sbin/mr-jobhistory-daemon.sh start historyserver
