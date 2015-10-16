#!/bin/sh
scriptName="compressMonth.sh"
echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") [$scriptName] Script starts"

dataDirectory="/datastore/complexnet/twitter/data/"
month=$(date -d 'a week ago' '+%Y-%m')

echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") [$scriptName] compressing files form $month"
echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") [$scriptName] tar czvf $dataDirectory$month.tgz $dataDirectory$month*"
tar czvf $dataDirectory$month.tgz $dataDirectory$month*

echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") [$scriptName] deleting files from $month"
echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") [$scriptName] rm $dataDirectory$month*.{data,deleted}"
rm $dataDirectory$month*.{data,deleted}
echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") [$scriptName] Script ends"
