#!/bin/bash
HOPS=/tmp/hops/hadoop
DEPLOY_FILE=hops.tgz
DEPLOY=/var/www/hops/$DEPLOY_FILE

rm $DEPLOY
rm -rf $HOPS
mkdir -p $HOPS

mvn -f ./../pom.xml  package -Pdist -Dtar -DskipTests

HADOOP=hadoop-2.0.4-alpha

tar xf ../hadoop-dist/target/$HADOOP.tar.gz -C $HOPS
cp -rf $HOPS/$HADOOP/* $HOPS/

rm -rf $HOPS/$HADOOP

cd $HOPS
cd ..

tar zcf $DEPLOY_FILE .
mv $DEPLOY_FILE $DEPLOY
