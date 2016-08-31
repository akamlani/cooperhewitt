import os
import boto

class AwsConnUtils(object):
    def __init__(self):
        self.export_path = './../export/'
        self.access_key = os.environ['AWS_ACCESS_KEY_ID']
        self.secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
        self.conn = boto.connect_ec2(self.access_key, self.secret_access_key)

    def get_public_dns(self, instance_name = 'spark_cluster-master'):
        reservations = self.conn.get_all_instances()
        instances = [inst for res in reservations for inst in res.instances]
        name_dns_mapping = [{'name': inst.__dict__['tags']['Name'], 'public_dns': inst.__dict__['public_dns_name']}
        for inst in instances if instance_name in inst.__dict__['tags']['Name'] ][0]
        return name_dns_mapping


# get master
aws = aws_utils.AwsConnUtils()
dns_mapping = aws.get_public_dns()
url = 'spark://' + str(dns_mapping['public_dns']) + ':7077'
