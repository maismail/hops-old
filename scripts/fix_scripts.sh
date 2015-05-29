#!/bin/bash
# Author: Salman Niazi 2014
# This script broadcasts all files required for running a HOP instance.
# A password-less sign-on should be setup prior to calling this script

source deployment_scripts/deployment.properties

PRIMARY_NN="bbc1.sics.se"
SECONDARY_NN="bbc2.sics.se"
ZKNODE="cloud1.sics.se"
JOURNAL_NODE_1="bbc3.sics.se"
JOURNAL_NODE_2="bbc4.sics.se"
JOURNAL_NODE_3="bbc5.sics.se"
LIST_OF_ALL_DN="cloud1.sics.se cloud2.sics.se cloud3.sics.se"
YARN_MASTER_NODE="bbc7.sics.se"
LIST_OF_ALL_EXP_MACHINES='cloud1.sics.se cloud2.sics.se cloud3.sics.se cloud4.sics.se cloud5.sics.se cloud6.sics.se cloud7.sics.se cloud8.sics.se cloud10.sics.se      cloud12.sics.se cloud14.sics.se cloud15.sics.se cloud16.sics.se bbc1.sics.se bbc2.sics.se bbc3.sics.se bbc4.sics.se bbc5.sics.se bbc6.sics.se bbc7.sics.se'        
JOURNALNODE_EDITS_DIR=$HOP_Dist_Folder/journal
FSIMAGE_DIR=$HOP_Dist_Folder/fsimageDir 



find ./deployment_scripts -iname  "*" -type f  -exec sed -i 's/PRIMARYNN/'$PRIMARY_NN'/g' {} \;
find ./deployment_scripts -iname  "*" -type f  -exec sed -i 's/SECONDARYNN/'$SECONDARY_NN'/g' {} \;
find ./deployment_scripts -iname  "*" -type f  -exec sed -i 's/ZKNODE/'$ZKNODE'/g' {} \;
find ./deployment_scripts -iname  "*" -type f  -exec sed -i 's/JOURNAL_NODE_1/'$JOURNAL_NODE_1'/g' {} \;
find ./deployment_scripts -iname  "*" -type f  -exec sed -i 's/JOURNAL_NODE_2/'$JOURNAL_NODE_2'/g' {} \;
find ./deployment_scripts -iname  "*" -type f  -exec sed -i 's/JOURNAL_NODE_3/'$JOURNAL_NODE_3'/g' {} \;
find ./deployment_scripts -iname  "*" -type f  -exec sed -i "s/LIST_OF_ALL_CLIENT_MACHINES_PLACE_HOLDER/$LIST_OF_ALL_EXP_MACHINES/g" {} \;
find ./deployment_scripts -iname  "*" -type f  -exec sed -i "s/LIST_OF_ALL_DATANODE_PLACE_HOLDER/$LIST_OF_ALL_DN/g" {} \;
find ./deployment_scripts -iname  "*" -type f  -exec sed -i "s/YARN_MASTER_PLACE_HOLDER/$YARN_MASTER_NODE/g" {} \;
find ./deployment_scripts -iname  "*" -type f  -exec sed -i 's|JOURNALNODE_EDITS_DIR|'$JOURNALNODE_EDITS_DIR'|g' {} \;
find ./deployment_scripts -iname  "*" -type f  -exec sed -i 's|FSIMAGE_DIR|'$FSIMAGE_DIR'|g' {} \;

