#!/bin/bash
rsync -av --progress --delete Repositories/hop ramdisk/
rsync -av --progress --delete Repositories/hdfs-erasure-coding ramdisk
