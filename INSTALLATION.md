```
Define environment variables in ~/.bash_profile or ~/.bashrc
- Root directory where this is cloned: COOPERHEWITT_ROOT
- Environment Installations: SPARK_HOME, JAVA_HOME, PYTHONPATH
- S3 Access Keys (if using serialized *.pkl files): AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
```

```
Acquire serialized files:
- Execute the following script: aws.py
  Note this requires AWS S3 keys to be set, as it will request from the existing bucket
```

```
Registration Configuration (per API Keys of Cooper Hewitt)
- Fill in the sections in config/api_cred_template.yml based on registration with Cooper Hewitt
- Rename the file to api_cred.yml and place in config folder
- In most places, serialized files are already built, so this is not required
```

```
Mongodb
- This is currently only required if you are not using the serialized *.pkl formatted files from the AWS bucket
- [Mongodb Installation on AWS](https://docs.mongodb.com/manual/tutorial/install-mongodb-on-amazon/)
```

```python
# setup environment (ideally this would be placed in the ipython profile)
# only required for src/{notebooks, apps} folders
import os
import sys
sys.path.append(os.environ['COOPERHEWITT_ROOT'] + '/src')
sys.path.append(os.environ['COOPERHEWITT_ROOT'] + '/src/apps')
```

```
Spark Specifics
- Launch a Cluster
    ${SPARK_HOME}/ec2/spark-ec2 -k awskeypair -i ~/.ssh/awskeypair.pem -r us-east-1
    -s 6 --copy-aws-credentials --ebs-vol-size=64 --instance-type="m3.xlarge" launch spark_cluster
- Start a Cluster
    ${SPARK_HOME}/ec2/spark-ec2 -k awskeypair -i ~/.ssh/awskeypair.pem -r us-east-1 start spark_cluster
- Stop  a Cluster
    ${SPARK_HOME}/ec2/spark-ec2 -k awskeypair -i ~/.ssh/awskeypair.pem -r us-east-1 stop spark_cluster
- Login to a Cluster
    ${SPARK_HOME}/ec2/spark-ec2 -k awskeypair -i ~/.ssh/awskeypair.pem -r us-east-1 login spark_cluster
- Start Jupyter Notebook on the master (note this includes the package graphframes)
    IPYTHON_OPTS="notebook --ip=0.0.0.0" ${SPARK_HOME}/bin/pyspark \
    --packages graphframes:graphframes:0.1.0-spark1.6 \
    --executor-memory 8G \
    --driver-memory 8G
- Monitors
    webport: <ip>:8080
    sparkui: <ip>:4040
- Unable to Start a Hive Context, perform the following:
    rm -rf ./metastore_db
    ~/ephemeral-hdfs/bin/hadoop fs -chmod 777 /tmp/hive
```
