#!/bin/sh
scriptName="checkAndLaunchDataCollection.sh"
echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") [$scriptName] Script starts"


echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") [$scriptName] checking data collection status"
nRunningProcesses=$(ps aux | grep dataCollection.py | grep jmague | grep -v grep | wc -l)
if [ $nRunningProcesses != 2 ]
  then
  echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") [$scriptName] data collection not running !"
  pkill -f dataCollection.py
  /home/jmague/anaconda/bin/python /home/jmague/SoSweet/dataCollection.py &
  echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") [$scriptName] launched"
  exit
else
  echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") [$scriptName] data collection is running"
fi

dataDirectory="/datastore/complexnet/twitter/data/"
actualFile=$(ls -1t $dataDirectory*.data | head -1)
expectedFile=$(date -u +$dataDirectory%Y-%m-%dT%H.data)
echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") [$scriptName] expected current data file: $expectedFile"
echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") [$scriptName] actual current data file: $actualFile"
if [ "$actualFile" != "$expectedFile" ]
  then
  echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") [$scriptName] files mismatch; relaunching data collection processes."
  pkill -f dataCollection.py
  /home/jmague/SoSweet/checkAndLaunchDataCollection.sh
else
  echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") [$scriptName] files match; data collection processes seems OK."
fi

echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") [$scriptName] Script ends"
