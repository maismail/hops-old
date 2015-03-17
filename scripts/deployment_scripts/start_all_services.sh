#!/bin/bash
# Author: Salman Niazi 2014
# This script broadcasts all files required for running a HOP instance.
# A password-less sign-on should be setup prior to calling this script

echo "------------->Wipe logs ... "
source wipe_logs.sh
echo "------------->Starting ZK ... "
source start_ZK.sh
echo "------------->Wipe Journals ... "
source wipe_JN.sh
echo "------------->Starting JNs ... "
source start_JN.sh
sleep 10
echo "------------->Formatting ... "
source formatNN.sh
echo "------------->Copying the FSImage Dirs to Secondary ... "
source copy_format_dirs.sh
echo "------------->Making ZNode ... "
sleep 3
source make_znode.sh
echo "------------->Wiping DNs ... "
source wipe_datanode_data_dir.sh
echo "------------->Starting NNs ... "
source start_NNs.sh
echo "------------->Starting DNs ... "
source start_DNs.sh
echo "------------->Starting Yarn ... "
source start_yarn.sh

exit 0



