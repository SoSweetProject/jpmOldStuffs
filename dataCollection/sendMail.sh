#!/bin/bash
scriptName="sendMail.sh"
echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") [$scriptName] Script starts"

echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") [$scriptName] emailing /datastore/complexnet/twitter/graphs/volume.png and /datastore/complexnet/twitter/graphs/users.png to jean-philippe.mague@ens-lyon.fr"
/home/jmague/anaconda/bin/pyzsendmail -f jean-philippe.mague@ens-lyon.fr -t jean-philippe.mague@ens-lyon.fr -s "SoSweet stats : volume and users" -a image/png:volume.png:/datastore/complexnet/twitter/graphs/volume.png -a image/png:users.png:/datastore/complexnet/twitter/graphs/users.png

echo "$(date -u +"%Y-%m-%dT%H:%M:%SZ") [$scriptName] Script ens"
