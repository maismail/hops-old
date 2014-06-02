#!/bin/bash
export HADOOP_PREFIX=/opt/hop/hadoop-2.0.4-alpha

$HADOOP_PREFIX/sbin/yarn-daemon.sh stop resourcemanager
$HADOOP_PREFIX/sbin/hadoop-daemon.sh stop namenode
$HADOOP_PREFIX/sbin/mr-jobhistory-daemon.sh stop historyserver
