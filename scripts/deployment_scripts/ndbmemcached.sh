#!/bin/bash
set +e

HOME=/home/jenkins/opt
PID=$HOME/ndbmemcached/ndbmemcached.pid
LOG=$HOME/ndbmemcached/ndbmemcached.log
SRVR=cloud1.sics.se:1186

if [ $1 = "start" ]
then
  jobs &>/dev/null
  $HOME/mysql-cluster/bin/memcached -E $HOME/mysql-cluster/lib/ndb_engine.so -e "connectstring=$SRVR;role=ndb-caching" -p 11212 -U 11212 -v > $LOG 2>&1 &
  new_job_started="$(jobs -n)"
  if [ -n "$new_job_started" ];then
     VAR=$!
  else
     VAR=
  fi
  echo $VAR > $PID
  exit 0
fi

if [ $1 = "stop" ]
then
  if [ -f $PID ]
  then
     kill -9 `cat $PID`
     rm $PID
  else
     echo "$PID is not found, you should start first"
  fi
  exit 0
fi

echo "You should use either start or stop"
