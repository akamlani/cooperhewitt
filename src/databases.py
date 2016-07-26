from pymongo import MongoClient


class Database(object):
    def __init__(self):
        # mongoimport --db cooperhewitt --collection museum <file>.json --batchSize 1
        self.client = MongoClient()
        self.db     = self.client.cooperhewitt
        self.coll_exhibitions = self.db.exhibitions
    
    def insert_records(self, records, name):
        if name == "exhibition":
            self.coll_exhibitions.insert_many(records)

    def query_records(self, name, query_id):
        if name == "exhibition":
            return self.coll_exhibitions.find_one({"_id": query_id} )
