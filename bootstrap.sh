#!/bin/bash

# Exit on error
set -e

# Print each command before executing it
set -o xtrace

OPENBENCHMARK_DIR="$( cd "$(dirname "$0")" ; pwd -P )"

# FIXME private branch, change to the official repo once code is merged
TAG_COAP=develop_COAP-44
TAG_OV=OV-7
REPO_COAP=https://github.com/malishav/coap.git
REPO_OV=https://github.com/malishav/openvisualizer.git

sudo apt-get update
sudo apt -y install build-essential
sudo apt -y install git

### Below is a verbatim copy of the OpenWSN install.sh script
# until a typo in openvisualizer installation gets fixed
mkdir openwsn
sudo apt-get install -y git
cd ./openwsn/
git clone https://github.com/openwsn-berkeley/openwsn-fw.git
git clone https://github.com/openwsn-berkeley/openvisualizer.git
git clone https://github.com/openwsn-berkeley/coap.git
cd ./openwsn-fw/
sudo apt-get install -y python-dev
sudo apt-get install -y scons
cd ../openvisualizer/
sudo apt-get install -y python-pip
sudo apt-get install -y python-tk
sudo pip install -r requirements.txt
cd ../coap/
sudo pip install -r requirements.txt
sudo apt-get install -y gcc-arm-none-eabi
sudo apt-get install -y gcc-msp430

cd ../..
### End of OpenWSN install.sh script copy

wget https://openwsn.atlassian.net/wiki/download/attachments/29196302/install.sh 
#bash install.sh
rm install.sh

# Update OpenWSN-CoAP with the correct commit name
cd ./openwsn/coap
git remote add -t $TAG_COAP -f repository $REPO_COAP
git checkout $TAG_COAP

# Update OpenVisualizer with the correct commit name
cd ../openvisualizer
git remote add -t $TAG_OV -f repository $REPO_OV
git checkout $TAG_OV

# Install OpenBenchmark requirements; OpenBenchmark scripts do not run with sudo
pip install -r $OPENBENCHMARK_DIR/requirements.txt --user

ssh-keygen -t rsa -N "" -f ~/.ssh/id_rsa
echo "==================================="
echo "Please publish the following SSH key on any server where automated SSH is requested"
cat ~/.ssh/id_rsa.pub
echo "==================================="

