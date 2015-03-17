#!/bin/bash
# Author: Salman Niazi 2014
# This script broadcasts all files required for running a HOP instance.
# A password-less sign-on should be setup prior to calling this script


#load config parameters
source deployment.properties

	ssh $HOP_User@cloud1.sics.se $HOP_Dist_Folder/bin/hdfs zkfc -formatZK






