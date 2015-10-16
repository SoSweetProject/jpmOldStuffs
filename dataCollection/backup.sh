#!/bin/sh
scriptName="backup.sh"
echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") [$scriptName] Script starts"

dataDirectory="/datastore/complexnet/twitter/data"
localBackupDirectory="/home/jmague/backup"
externalBackupDirectry="twitter.cbp.ens-lyon.fr:/home/jmague/SoSweet/backup"

mkdir -p $localBackupDirectory/data
echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") [$scriptName] new level 1 file /home/jmague/SoSweet/backup/backup_`date +%F`.tar.gz"
echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") [$scriptName] find $dataDirectory -name "*.deleted" -o -name "*.data" -o -name "*.tgz" | tar --create --verbose --listed-incremental $localBackupDirectory/data/data.snar --file $localBackupDirectory/data/backup_`date +%F`.tar -T -"
find $dataDirectory -name "*.deleted" -o -name "*.data" -o -name "*.tgz" | tar --create --verbose --listed-incremental $localBackupDirectory/data/data.snar --file $localBackupDirectory/data/backup_`date +%F`.tar -T -

echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") [$scriptName] copying databases"
echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") [$scriptName] cp $dataDirectory/*.db $localBackupDirectory/data"
cp $dataDirectory/*.db $localBackupDirectory/data

echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") [$scriptName] taring"
echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") [$scriptName] tar --create --verbose --gzip --file $localBackupDirectory/backup.tar $localBackupDirectory/data"
tar --create --verbose --gzip --file $localBackupDirectory/backup.tar $localBackupDirectory/data

echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") [$scriptName] copying /home/jmague/SoSweet/backup/backup.tar to twitter.cbp.ens-lyon.fr:/home/jmague/SoSweet/backup/backup.tar"
echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") [$scriptName] scp $localBackupDirectory/backup.tar $externalBackupDirectry"
scp $localBackupDirectory/backup.tar $externalBackupDirectry

echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") [$scriptName] Script ends"
