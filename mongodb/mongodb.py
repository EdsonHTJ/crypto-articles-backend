import os
from pymongo.server_api import ServerApi
from pymongo.mongo_client import MongoClient
import random


def get_mongo_uri():
    user = os.getenv('MONGO_USER')
    password = os.getenv('MONGO_PASSWD')
    return f"mongodb+srv://{user}:{password}@cluster0.yb4m7.mongodb.net/?retryWrites=true&w=majority"


class Database:
    def __init__(self) -> None:
        self.uri = get_mongo_uri()
        self.collection = "test_col"
        self.db = "test_db"
        self.client = MongoClient(
            self.uri, server_api=ServerApi('1'))

    def insert_one(self, data: dict):
        self.client[self.db][self.collection].insert_one(data)
        pass

    def get_one(self):
        return self.client[self.db][self.collection].find_one({"klv_sentiment": "undefined"})

    def get_random(self):
        count = self.client[self.db][self.collection].count_documents(
            {"klv_sentiment": "undefined"})
        if count == 0:
            return None
        random_offset = random.randint(0, count - 1)
        return self.client[self.db][self.collection].find({"klv_sentiment": "undefined"}).skip(random_offset).limit(1).next()

    def get_all(self):
        return self.client[self.db][self.collection].find({})
