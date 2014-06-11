#!/bin/bash
# Author: Salman Niazi 2014
# This script broadcasts all files required for running a HOP instance.
# A password-less sign-on should be setup prior to calling this script

source deployment.properties

#upload Experiments
if [ $HOP_Upload_Experiments = true ]; then
   echo "***   Copying the Experiment to $HOP_Experiments_Dist_Folder ***"
	for machine in $HOP_Experiments_Machine_List
	do
		 connectStr="$HOP_User@$machine"
		 ssh $connectStr 'mkdir -p '$HOP_Experiments_Dist_Folder
		 
		 RunScriptFile=$HOP_Experiments_Dist_Folder/run.sh	
                 JarFileName=hop-experiments-1.0-SNAPSHOT-jar-with-dependencies.jar
                 ssh $connectStr 'touch '                            $RunScriptFile
		 ssh $connectStr "echo  \#\!/bin/bash > "              $RunScriptFile
		 ssh $connectStr "echo  $HOP_Dist_Folder/bin/hadoop jar $JarFileName  $\* >>"   $RunScriptFile
		 ssh $connectStr 'chmod +x '$RunScriptFile
	done	
	parallel-rsync -arz -H "${HOP_Experiments_Machine_List[*]}" --user $HOP_User     $HOP_Experiments_Folder/target/$JarFileName   $HOP_Experiments_Dist_Folder  
fi

