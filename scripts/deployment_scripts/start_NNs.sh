#!/bin/bash
# Author: Salman Niazi 2014
# This script broadcasts all files required for running a HOP instance.
# A password-less sign-on should be setup prior to calling this script


#load config parameters
source deployment.properties

#All Unique Hosts
All_Hosts=${HOP_Default_NN[*]}" "${HOP_NN_List[*]}
All_Unique_Hosts=$(echo "${All_Hosts[@]}" | tr ' ' '\n' | sort -u | tr '\n' ' ')

FIRST=1

for i in ${All_Unique_Hosts[@]}
do
	connectStr="$HOP_User@$i"
	if [ $FIRST -eq 1 ]
        then
	   ssh $connectStr $HOP_Dist_Folder/bin/hdfs namenode -safeModeFix
	   FIRST=0
           echo "Cluster will enter safemode"
	fi
	echo "Starting NN on $i"
	ssh $connectStr $HOP_Dist_Folder/sbin/hadoop-daemon.sh --script hdfs start namenode
done
