#!/bin/bash
ssh cloud8 "./stop-master.sh"
./stop-slaves.sh
mysql -u hop -phop -h cloud1 -P3307 -e "drop database hop_steffen";
mysql -u hop -phop -h cloud1 -P3307 -e "create database hop_steffen";
mysql -u hop -phop -h cloud1 -P3307 hop_steffen < schema_backup.sql;
ssh cloud8 "./format-namenode.sh"
./clean-logs.sh
./format-datanodes.sh
