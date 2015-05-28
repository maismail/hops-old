#!/bin/bash
# Author: Salman Niazi 2014
# This script broadcasts all files required for running a HOP instance.
# A password-less sign-on should be setup prior to calling this script


#load config parameters
source deployment.properties


ssh root@bbc3.sics.se $HOP_Dist_Folder/sbin/hadoop-daemon.sh start journalnode
ssh root@bbc4.sics.se $HOP_Dist_Folder/sbin/hadoop-daemon.sh start journalnode
ssh root@bbc5.sics.se $HOP_Dist_Folder/sbin/hadoop-daemon.sh start journalnode



