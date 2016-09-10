import os
import pandas as pd
import boto

class AwsConn(object):
    def __init__(self):
        root_path = os.environ['COOPERHEWITT_ROOT']
        self.export_path = root_path + "/export/"
        self.access_key = os.environ['AWS_ACCESS_KEY_ID']
        self.secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
        self.conn = boto.connect_s3(self.access_key, self.secret_access_key)

    def acquire_files(self, path_name):
        bucket_names = [b.name for b in self.conn.get_all_buckets()]
        bucket = self.conn.get_bucket(bucket_names[1])
        filenames = [f.name for f in bucket.list()]
        filenames = [f for f in filenames if path_name in f and f != path_name]
        return filenames

    def create_resource(self, bucket_name, path_name, resource_file):
        export_path = self.export_path + resource_file
        bucket = self.conn.get_bucket(bucket_name)
        key = bucket.get_key(path_name + resource_file)
        # assuming we have enough space, else create symbolic link beforehand
        if not os.path.exists(self.export_path):
            os.mkdir(self.export_path)
        if not os.path.isfile(export_path):
            print export_path, path_name + resource_file,  key
            with open(export_path, 'wb') as f: key.get_contents_to_file(f)

    def get_public_dns(self, instance_name = 'spark_cluster-master'):
        reservations = self.conn.get_all_instances()
        instances = [inst for res in reservations for inst in res.instances]
        name_dns_mapping = [{'name': inst.__dict__['tags']['Name'], 'public_dns': inst.__dict__['public_dns_name']}
        for inst in instances if instance_name in inst.__dict__['tags']['Name'] ][0]
        return name_dns_mapping



if __name__ == '__main__':
    path_name    = 'projects/cooper-hewitt/data/'
    bucket_name  = 'akamlani-galvanize'
    # code for AWS Connection buckets
    conn = AwsConn()
    filenames = [filename.replace(path_name, '') for filename in conn.acquire_files(path_name)]
    [conn.create_resource(bucket_name, path_name, filename) for filename in filenames]
    # log the public dns url 
    dns_mapping = conn.get_public_dns()
    url = 'spark://' + str(dns_mapping['public_dns']) + ':7077'
    print url
