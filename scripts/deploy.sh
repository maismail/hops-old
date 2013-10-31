#!/bin/bash
HOPS=/tmp/hops/hadoop
DEPLOY=/var/www/hops/hops.tgz

rm $DEPLOY
rm -rf $HOPS
mkdir -p $HOPS

COMMON=hadoop-common-2.0.4-alpha 
HDFS=hadoop-hdfs-2.0.4-alpha
CLIENT=hadoop-client-2.0.4-alpha
YARN=hadoop-yarn-project-2.0.4-alpha
MR=hadoop-mapreduce-2.0.4-alpha

tar xf ../hadoop-common-project/hadoop-common/target/$COMMON.tar.gz -C $HOPS
tar xf ../hadoop-hdfs-project/hadoop-hdfs/target/$HDFS.tar.gz -C $HOPS
tar xf ../hadoop-client/target/$CLIENT.tar.gz -C $HOPS
tar xf ../hadoop-mapreduce-project/target/$MR.tar.gz -C $HOPS
tar xf ../hadoop-yarn-project/target/$YARN.tar.gz -C $HOPS

cp -rf $HOPS/$COMMON/* $HOPS/
cp -rf $HOPS/$HDFS/* $HOPS/
cp -rf $HOPS/$CLIENT/* $HOPS/
cp -rf $HOPS/$YARN/* $HOPS/
cp -rf $HOPS/$MR/* $HOPS/
rm -rf $HOPS/$COMMON
rm -rf $HOPS/$HDFS
rm -rf $HOPS/$CLIENT
rm -rf $HOPS/$YARN
rm -rf $HOPS/$MR

cd $HOPS
cd ..
tar zcf $DEPLOY .
