conda install matplotlib
conda install seaborn
conda install system
conda install networkx
conda install pymongo
conda install dill
sudo apt-get update
sudo apt-get install -y python-qt4 git
sudo apt-add-repository ppa:webupd8team/java
sudo apt-get update
sudo apt-get install -y oracle-java8-installer
sudo apt-get install -y python-setuptools python-dev python-pip build-essential
sudo pip install py4j
sudo pip install flask
sudo pip install pathos
# mongodb 
sudo mkdir -p /data/db
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv EA312927
echo "deb http://repo.mongodb.org/apt/ubuntu trusty/mongodb-org/3.2 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-3.2.list
sudo apt-get update
sudo apt-get install -y mongodb-org
sudo service mongod start

# directory structure
mkdir ~/scripts

# environment variables

