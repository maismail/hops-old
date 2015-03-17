#!/bin/bash
# Author: Salman Niazi 2014
# This script broadcasts all files required for running a HOP instance.
# A password-less sign-on should be setup prior to calling this script


#load config parameters
source deployment.properties

        ssh $HOP_User@cloud1.sics.se "tar cjf  $HOP_Dist_Folder/fsimageDir.tar  -C  $HOP_Dist_Folder/fsimageDir . "
	scp $HOP_User@cloud1.sics.se:/$HOP_Dist_Folder/fsimageDir.tar $HOP_User@cloud2.sics.se:/$HOP_Dist_Folder
	
	ssh $HOP_User@cloud2.sics.se 'rm -rf '  $HOP_Dist_Folder/fsimageDir
	ssh $HOP_User@cloud2.sics.se 'mkdir -p '  $HOP_Dist_Folder/fsimageDir
	ssh $HOP_User@cloud2.sics.se 'tar -xf  '  $HOP_Dist_Folder/fsimageDir.tar -C $HOP_Dist_Folder/fsimageDir
        
        ssh $HOP_User@cloud2.sics.se $HOP_Dist_Folder/bin/hdfs namenode -bootstrapStandby











