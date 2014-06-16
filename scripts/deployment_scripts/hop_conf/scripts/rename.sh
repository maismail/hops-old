#!/bin/bash

config_params=$1
namenode=$2

source $config_params

hdfs_site_xml=$HOP_Dist_Folder/hop_conf/hdfs_configs/hdfs-site.xml
core_site_xml=$HOP_Dist_Folder/hop_conf/hdfs_configs/core-site.xml


sed -i 's|NN_MACHINE_NAME|'$namenode'|g' $core_site_xml
sed -i 's|Dfs_Namenode_Rpc_Address_Config_Param|'$Dfs_Namenode_Rpc_Address_Config_Param'|g' $core_site_xml


sed -i 's|NN_MACHINE_NAME|'$namenode'|g' $hdfs_site_xml
sed -i 's|Dfs_Namenode_Logging_Level_Config_Param|'$Dfs_Namenode_Logging_Level_Config_Param'|g' $hdfs_site_xml
sed -i 's|Dfs_Namenode_Http_Address_Config_Param|'$Dfs_Namenode_Http_Address_Config_Param'|g' $hdfs_site_xml
sed -i 's|Dfs_Namenode_Rpc_Address_Config_Param|'$Dfs_Namenode_Rpc_Address_Config_Param'|g' $hdfs_site_xml
sed -i 's|Dfs_Namenode_Servicerpc_Address_Config_Param|'$Dfs_Namenode_Servicerpc_Address_Config_Param'|g' $hdfs_site_xml
sed -i 's|Dfs_Datanode_Address_Config_Param|'$Dfs_Datanode_Address_Config_Param'|g' $hdfs_site_xml
sed -i 's|Dfs_Datanode_Http_Address_Config_Param|'$Dfs_Datanode_Http_Address_Config_Param'|g' $hdfs_site_xml
sed -i 's|Dfs_Datanode_Ipc_Address_Config_Param|'$Dfs_Datanode_Ipc_Address_Config_Param'|g' $hdfs_site_xml
sed -i 's|Dfs_BlockSize_Config_Param|'$Dfs_BlockSize_Config_Param'|g' $hdfs_site_xml
sed -i 's|Dfs_Datanode_Data_Dir_Config_Param|'$Dfs_Datanode_Data_Dir_Config_Param'|g' $hdfs_site_xml



exit 0
