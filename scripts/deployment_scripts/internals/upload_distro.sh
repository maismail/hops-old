#!/bin/bash
# Author: Salman Niazi 2014, Jim 2013
# This script broadcasts all files required for running a HOP instance.
# A password-less sign-on should be setup prior to calling this script

source deployment.properties

#All Unique Hosts
All_Hosts=${HOP_Default_NN[*]}" "${HOP_NN_List[*]}" "${HOP_DN_List[*]}" "${HOP_Experiments_Machine_List[*]}
All_Unique_Hosts=$(echo "${All_Hosts[@]}" | tr ' ' '\n' | sort -u | tr '\n' ' ')

#All Unique Namenodes
All_NNs=${HOP_Default_NN[*]}" "${HOP_NN_List[*]}
All_Unique_NNs=$(echo "${All_NNs[@]}" | tr ' ' '\n' | sort -u | tr '\n' ' ')

All_Hosts_Excluding_NNs=${HOP_DN_List[*]}" "${HOP_Experiments_Machine_List[*]}
All_Unique_Hosts_Excluding_NNs=$(echo "${All_Hosts_Excluding_NNs[@]}" | tr ' ' '\n' | sort -u | tr '\n' ' ')

echo "Deploying on ${All_Unique_Hosts[*]}"
echo "Distribution Folder: $HOP_Dist_Folder"


# create remote directories
for machine in $All_Unique_Hosts
do
 connectStr="$HOP_User@$machine"
 ssh $connectStr 'mkdir -p '$HOP_Dist_Folder
done



# copy data to the remote directories
echo "***   Copying the Distro   ***"
mkdir -p $HOP_Src_Folder/hadoop-dist/target/$Hadoop_Version/Java
cp -nr    $JAVA_Folder/*                                                         $HOP_Src_Folder/hadoop-dist/target/$Hadoop_Version/Java                                                                   
cp       $LIB_NDB_CLIENT_BIN                                                     $HOP_Src_Folder/hadoop-dist/target/$Hadoop_Version/lib/native 
cp -rf   $HOP_Src_Folder/scripts/deployment_scripts/hop_conf                     $HOP_Src_Folder/hadoop-dist/target/$Hadoop_Version/
cp       $HOP_Src_Folder/scripts/deployment_scripts/deployment.properties        $HOP_Src_Folder/hadoop-dist/target/$Hadoop_Version/hop_conf/
parallel-rsync -arzv -H "${All_Unique_Hosts[*]}" --user $HOP_User    $HOP_Src_Folder/hadoop-dist/target/$Hadoop_Version/        $HOP_Dist_Folder


# fix scripts on remote NN hosts
echo "***   Fixing address in NameNode configs   ***" 
for i in ${All_Unique_NNs[@]}
do
	connectStr="$HOP_User@$i"
	# running some scripts and commands on the server
	ssh $connectStr 'chmod +x       '$HOP_Dist_Folder'/hop_conf/scripts/*.sh'
	ssh $connectStr                 ''$HOP_Dist_Folder'/hop_conf/scripts/rename.sh   '$HOP_Dist_Folder/hop_conf/deployment.properties'    '$i' '
done

# fix scripts on remote DN hosts
echo "***   Fixing address in DataNode and Experiments configs   ***" 
for i in ${All_Unique_Hosts_Excluding_NNs[@]}
do
	connectStr="$HOP_User@$i"
	# running some scripts and commands on the server
	ssh $connectStr 'chmod +x       '$HOP_Dist_Folder'/hop_conf/scripts/*.sh'
	ssh $connectStr                 ''$HOP_Dist_Folder'/hop_conf/scripts/rename.sh   '$HOP_Dist_Folder/hop_conf/deployment.properties'    '${HOP_Default_NN[0]}' '
done

# move config files and scripts to appropriate folders
echo "***   Copying hdfs-site.xml core-site.xml etc to correct folders   ***" 
for i in ${All_Unique_Hosts[@]}
do
	connectStr="$HOP_User@$i"
	#copy the config files to right folders	
	ssh $connectStr cp         $HOP_Dist_Folder/hop_conf/hdfs_configs/*                              $HOP_Dist_Folder/etc/hadoop/
	

	#copying the script files to sbin
	ssh $connectStr cp -r            $HOP_Dist_Folder/hop_conf/scripts                               $HOP_Dist_Folder/sbin/
done




