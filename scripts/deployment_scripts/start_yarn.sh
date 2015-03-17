#!/bin/bash
# Author: Salman Niazi 2014
# This script broadcasts all files required for running a HOP instance.
# A password-less sign-on should be setup prior to calling this script


#load config parameters
source deployment.properties

connectStr="$HOP_User@${YARN_MASTER[0]}"
echo "Stopping ResourceManager, ProxyServer and HistoryServer on ${YARN_MASTER[0]}"
ssh $connectStr  $HOP_Dist_Folder/sbin/yarn-daemon.sh start resourcemanager &
ssh $connectStr  $HOP_Dist_Folder/sbin/yarn-daemon.sh start proxyserver &
ssh $connectStr  $HOP_Dist_Folder/sbin/mr-jobhistory-daemon.sh start historyserver &


for i in ${HOP_DN_List[@]}
do
	echo "Stopping NodeManager on $i"
	connectStr="$HOP_User@$i"
	ssh $connectStr  $HOP_Dist_Folder/sbin/yarn-daemon.sh start nodemanager &
done
