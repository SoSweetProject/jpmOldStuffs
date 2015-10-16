#!/bin/bash
scriptName="checkAndLaunchUpdateUsersCollection.sh"
echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") [$scriptName] Script stats"

# PYTHON=/Users/jmague/anaconda/bin/python
# SRC=/Users/jmague/Documents/Projets/SoSweet/python
PYTHON=/home/jmague/anaconda/bin/python
SRC=/home/jmague/SoSweet


function killProcess {
  pids=$(ps aux | grep $1 | grep -v grep | awk '{print $2}')
  if [[ $pids ]]
  then
    echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") [$scriptName] $(ps aux | grep $1 | grep -v grep | wc -l) processes"
    for pid in $pids
    do
     echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") [$scriptName] killing $1 ($pid) with SIGTERM"
     kill -15 $pid
     counter=0
     while [ "$(ps x | grep $pid | grep -v grep)" ]; do
       echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") [$scriptName] $1 ($pid) is still running; wainting $(($2 - $counter)) more seconds before sending SIGKILL"
       sleep 1;
       counter=$((counter + 1));
       if [ $counter -eq $2 ]; then
         break
       fi
     done
     if [ $counter -ne $2 ]; then
       echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") [$scriptName] $1 killed with SIGTERM."
     else
      kill -9 $pid
      echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") [$scriptName] $1 killed with SIGKILL."
     fi
   done
  else
    echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") [$scriptName] $1 is not running"
  fi
}



killProcess updateUsersFriends.py 15
killProcess updateUsersCollection.py 15
killProcess stats.py 15


echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") [$scriptName] launching updateUsersCollection.py"
$PYTHON $SRC/updateUsersCollection.py --path_to_data /datastore/complexnet/twitter/data/ --database /datastore/complexnet/twitter/data/users.db
echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") [$scriptName] updateUsersCollection.py done."

echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") [$scriptName] launching stats.py"
$PYTHON $SRC/stats.py
echo $(date -u +"%Y-%m-%dT%H:%M:%SZ") [$scriptName] "stats.py done."
$PYTHON $SRC/statServer.py

echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") [$scriptName] launching updateUsersFriends.py"
$PYTHON $SRC/updateUsersFriends.py --database /datastore/complexnet/twitter/data/users.db &

echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") [$scriptName] Script ends"
