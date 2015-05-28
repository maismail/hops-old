#!/bin/bash
# Author: Salman Niazi 2014
# This script broadcasts all files required for running a HOP instance.
# A password-less sign-on should be setup prior to calling this script

source deployment.properties

#upload Experiments

        echo "***   Copying the Experiment to $HOP_Experiments_Dist_Folder  on ${HOP_Experiments_Machine_List[*]}***"
	for machine in ${HOP_Experiments_Machine_List[*]}
	do
		 connectStr="$HOP_User@$machine"
		 ssh $connectStr 'mkdir -p '$HOP_Experiments_Dist_Folder
	done
		
	COMMAND="$HOP_Dist_Folder/Java/bin/java -Xmx2000m  -cp \"/tmp/nzo/hadoop-distributions/hadoop-2.4.0/share/hadoop/common/*:/tmp/nzo/hadoop-distributions/hadoop-2.4.0/share/hadoop/common/lib/*\" -Djava.net.preferIPv4Stack=true -Dhadoop.log.dir=$HOP_Dist_Folder/logs -Dhadoop.log.file=hadoop.log -Dhadoop.home.dir=$HOP_Dist_Folder -Dhadoop.id.str=nzo -Dhadoop.root.logger=WARN,console -Djava.library.path=$HOP_Dist_Folder/lib/native -Dhadoop.policy.file=hadoop-policy.xml -Djava.net.preferIPv4Stack=true -Dhadoop.security.logger=INFO,NullAppender  org.apache.hadoop.util.RunJar "
	
	COMMAND="$HOP_Dist_Folder/bin/hadoop jar "
		
	JarFileName=hop-experiments-1.0-SNAPSHOT-jar-with-dependencies.jar
	temp_folder=/tmp/hop_exp_distro
	rm -rf $temp_folder
	mkdir -p $temp_folder	
	cp $HOP_Experiments_Folder/target/$JarFileName $temp_folder/
	
	#[S] silly code
	#create executable for bench-marks
	RunScriptFile=$temp_folder/bech-mark.sh	
	touch $RunScriptFile
        echo  \#\!/bin/bash >                  $RunScriptFile
        echo  $COMMAND $JarFileName se.sics.hop.experiments.BenchMark $\* >>   $RunScriptFile
        chmod +x $RunScriptFile
        
        
        RunScriptFile=$temp_folder/rename-sto-bech-mark.sh	
	touch $RunScriptFile
        echo  \#\!/bin/bash >                  $RunScriptFile
        echo   $COMMAND   $JarFileName se.sics.hop.experiments.RenameDir $\* >>   $RunScriptFile
        chmod +x $RunScriptFile
                 
        RunScriptFile=$temp_folder/delete-sto-bech-mark.sh	
	touch $RunScriptFile
        echo  \#\!/bin/bash >                  $RunScriptFile
        echo   $COMMAND  $JarFileName se.sics.hop.experiments.DeleteDir $\* >>   $RunScriptFile
        chmod +x $RunScriptFile
                 
	parallel-rsync -arz -H "${HOP_Experiments_Machine_List[*]}" --user $HOP_User     $temp_folder/   $HOP_Experiments_Dist_Folder  
