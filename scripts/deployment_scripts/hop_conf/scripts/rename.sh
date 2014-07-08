#!/bin/bash

config_params=$1
namenode=$2

source $config_params

hdfs_site_xml=$HOP_Dist_Folder/hop_conf/hdfs_configs/hdfs-site.xml
core_site_xml=$HOP_Dist_Folder/hop_conf/hdfs_configs/core-site.xml
yarn_site_xml=$HOP_Dist_Folder/hop_conf/hdfs_configs/yarn-site.xml

#making changes in core-site.xml
sed -i 's|NN_MACHINE_NAME|'$namenode'|g' $core_site_xml


#making changes in hdfs-site.xml
sed -i 's|NN_MACHINE_NAME|'$namenode'|g' $hdfs_site_xml
sed -i 's|Dfs_Namenode_Logging_Level_Config_Param|'$Dfs_Namenode_Logging_Level_Config_Param'|g' $hdfs_site_xml
port=$Dfs_Port_Param
sed -i 's|Dfs_Namenode_Http_Address_Config_Param|'$port'|g' $hdfs_site_xml
port=$((port + 1))
sed -i 's|Dfs_Namenode_Rpc_Address_Config_Param|'$port'|g' $hdfs_site_xml
sed -i 's|Dfs_Namenode_Rpc_Address_Config_Param|'$port'|g' $core_site_xml
port=$((port + 1))
sed -i 's|Dfs_Namenode_Servicerpc_Address_Config_Param|'$port'|g' $hdfs_site_xml
port=$((port + 1))
sed -i 's|Dfs_Datanode_Address_Config_Param|'$port'|g' $hdfs_site_xml
port=$((port + 1))
sed -i 's|Dfs_Datanode_Http_Address_Config_Param|'$port'|g' $hdfs_site_xml
port=$((port + 1))
sed -i 's|Dfs_Datanode_Ipc_Address_Config_Param|'$port'|g' $hdfs_site_xml

sed -i 's|Dfs_BlockSize_Config_Param|'$Dfs_BlockSize_Config_Param'|g' $hdfs_site_xml
sed -i 's|Dfs_Datanode_Data_Dir_Config_Param|'$Dfs_Datanode_Data_Dir_Config_Param'|g' $hdfs_site_xml



#making changes in yarn-site.xml
sed -i 's|YARN_MASTER|'$YARN_MASTER'|g' $yarn_site_xml
port=$Yarn_Port_Param
sed -i 's|PORT_1|'$port'|g' $yarn_site_xml
port=$((port + 1))
sed -i 's|PORT_2|'$port'|g' $yarn_site_xml
port=$((port + 1))
sed -i 's|PORT_2|'$port'|g' $yarn_site_xml
port=$((port + 1))
sed -i 's|PORT_3|'$port'|g' $yarn_site_xml
port=$((port + 1))
sed -i 's|PORT_4|'$port'|g' $yarn_site_xml
port=$((port + 1))
sed -i 's|PORT_5|'$port'|g' $yarn_site_xml
port=$((port + 1))
sed -i 's|PORT_6|'$port'|g' $yarn_site_xml
port=$((port + 1))
sed -i 's|PORT_7|'$port'|g' $yarn_site_xml
port=$((port + 1))
sed -i 's|PORT_8|'$port'|g' $yarn_site_xml
port=$((port + 1))
sed -i 's|PORT_9|'$port'|g' $yarn_site_xml


exit 0
