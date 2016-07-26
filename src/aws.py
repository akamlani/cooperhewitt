import os
import pandas as pd
import boto

class AwsConn(object):
    def __init__(self):
        self.export_path = './../export/' 
        self.access_key = os.environ['AWS_ACCESS_KEY_ID']
        self.secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
        self.conn = boto.connect_s3(self.access_key, self.secret_access_key)

    def acquire_files(self, path='projects/cooper-hewitt/data/'):
        bucket_names = [b.name for b in self.conn.get_all_buckets()]
        bucket = self.conn.get_bucket(bucket_names[1])
        filenames = [f.name for f in bucket.list()]
        filenames = [f for f in filenames if 'projects/cooper-hewitt/data/' in f]
        return filenames

    def create_resource(self, resource_file,
                        bucket_name='akamlani-galvanize',
                        data_path='projects/cooper-hewitt/data/'):
        export_path = self.export_path + resource_file
        bucket = self.conn.get_bucket(bucket_name)
        key = bucket.get_key(data_path + resource_file)
        if not os.path.exists(self.export_path):
            os.mkdir("./../export")
        if not os.path.isfile(export_path):
	    print export_path, data_path + resource_file,  key
	    with open(export_path, 'wb') as f: key.get_contents_to_file(f)
        return export_path



# code for AWS Connection buckets
aws = AwsConn()
res = {"collection_objects": ('collection_objects.pkl', 'pkl'),
       "exhibition_objects": ('exhibition_objects.pkl', 'pkl'),
       "exhibitions":        ('exhibitions.pkl', 'pkl'),
       "pen":                ('pen_collected_items.csv', 'csv'),
       "pen_tr_raw":         ('pen_transformed_raw.pkl', 'pkl'),
       "pen_tr_features":    ('pen_transformed_features.pkl', 'pkl')}

file_res = {k: aws.create_resource(file) for k, (file, file_type) in res.iteritems() }

