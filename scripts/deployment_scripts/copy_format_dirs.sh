#!/bin/bash
# Author: Salman Niazi 2014
# This script broadcasts all files required for running a HOP instance.
# A password-less sign-on should be setup prior to calling this script


#load config parameters
source deployment.properties

    ssh $HOP_User@bbc1.sics.se "tar cjf  $HOP_Dist_Folder/fsimageDir.tar  -C  /tmp/nzo/ref/apache_hadoop_distro/hadoop-2.0.4-alpha/fsimageDir . "
    rm -rf /tmp/fsimageDir.tar
	scp $HOP_User@bbc1.sics.se:/$HOP_Dist_Folder/fsimageDir.tar /tmp/fsimageDir.tar
	scp /tmp/fsimageDir.tar $HOP_User@bbc2.sics.se:/$HOP_Dist_Folder
	
	ssh $HOP_User@bbc2.sics.se 'rm -rf '    /tmp/nzo/ref/apache_hadoop_distro/hadoop-2.0.4-alpha/fsimageDir
	ssh $HOP_User@bbc2.sics.se 'mkdir -p '  /tmp/nzo/ref/apache_hadoop_distro/hadoop-2.0.4-alpha/fsimageDir
	ssh $HOP_User@bbc2.sics.se 'tar -xf  '  $HOP_Dist_Folder/fsimageDir.tar -C /tmp/nzo/ref/apache_hadoop_distro/hadoop-2.0.4-alpha/fsimageDir
        
    ssh $HOP_User@bbc2.sics.se $HOP_Dist_Folder/bin/hdfs namenode -bootstrapStandby











