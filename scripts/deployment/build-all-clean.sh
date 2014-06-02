#!/bin/bash
export SOURCE_PREFIX=/home/steffeng/ramdisk

cd $SOURCE_PREFIX/hop/hop-metadata-dal
git pull
mvn clean install
cd $SOURCE_PREFIX/hop/hop-metadata-dal-impl-ndb
git pull
mvn clean install
mvn assembly:assembly
cd $SOURCE_PREFIX/hop/hop
git pull
mvn clean install -DskipTests
cd $SOURCE_PREFIX/hdfs-erasure-coding
git pull
mvn clean generate-sources install -DskipTests
mvn assembly:assembly -DskipTests
cd $SOURCE_PREFIX/hop/hop
mvn package -Pdist -DskipTests=true
cp $SOURCE_PREFIX/hop/hop-metadata-dal-impl-ndb/target/hop-metadata-dal-impl-ndb-1.0-SNAPSHOT-jar-with-dependencies.jar $SOURCE_PREFIX/hop/hop/hadoop-dist/target/hadoop-2.0.4-alpha/share/hadoop/hdfs/lib/
cp $SOURCE_PREFIX/hdfs-erasure-coding/target/erasure-coding-1.0-SNAPSHOT-jar-with-dependencies.jar $SOURCE_PREFIX/hop/hop/hadoop-dist/target/hadoop-2.0.4-alpha/share/hadoop/hdfs/lib/
