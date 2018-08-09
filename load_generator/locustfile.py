#!/usr/bin/env python
import uuid
import random
import string
import gevent
import datetime
from es_locust import ElasticsearchRpcLocust
from locust import HttpLocust, TaskSet, task, events

WORDS = ['heavy', 'light', 'common', 'agent', 
         'word', 'api', 'retail', 'code', 
         'python', 'programming', 'hello', 'greeting', 
         'terminal', 'camera', 'brain', 'eyes', 
         'mouth', 'speech', 'king', 'queen', 
         'chess', 'computer', 'book', 'magazine', 
         'rubber', 'paper', 'article', 'language',
         'help', 'beatles', 'music', 'sing', 
         'street', 'race', 'condition', 'if']

def generate_payload(length):
    random_payload = ''.join([random.choice(string.ascii_letters) for _ in range(length)])
    words_payload = ' '.join([random.choice(WORDS) for _ in range(5)])
    return '{} {}'.format(words_payload, random_payload)

def guid():
    return str(uuid.uuid4())

HeavyIndexName = 'test_heavy_index'
LightIndexName = 'test_light_index'
DefaultType = 'default_type'

class UserTaskSet(TaskSet):
    def on_start(self):
        self.user_id = guid()
        self.indexed_docs = {
            HeavyIndexName: [],
            LightIndexName: []
        }

    def index_task(self, index_name):
        document_id = guid()
        document = {
            'document_id': document_id,
            'payload': generate_payload(25),
            'user_id': self.user_id,
            'date': str(datetime.datetime.now())
        }

        self.indexed_docs[index_name].append(document)
        self.client.index(index=index_name, doc_type=DefaultType, id=document_id, body=document)

    def update_task(self, index_name):
        if not self.indexed_docs[index_name]:
            return
        document = random.choice(self.indexed_docs[index_name])
        document['payload'] = generate_payload(25)
        document['date'] = str(datetime.datetime.now())
        self.client.update(index=index_name, doc_type=DefaultType, id=document['document_id'], body={'doc': document})

    def search_task(self, index_name):
        self.client.search(index=index_name, doc_type=DefaultType, body={
            "query": {
                "match": {
                    "payload": random.choice(WORDS)
                }
            }
        })

    @task(100)
    def index_heavy(self):
        self.index_task(HeavyIndexName)

    @task(20)
    def update_heavy(self):
        self.update_task(HeavyIndexName)

    @task(20)
    def search_heavy(self):
        self.search_task(HeavyIndexName)

    @task(5)
    def index_light(self):
        self.index_task(LightIndexName)

    @task(2)
    def update_light(self):
        self.update_task(LightIndexName)

    @task(2)
    def search_light(self):
        self.search_task(LightIndexName)

    @task(1)
    def stop(self):
        self.check_consistency()
        self.interrupt()

    def check_consistency(self):
        for index in [HeavyIndexName, LightIndexName]:
            for document in self.indexed_docs[index]:
                self.client.get_ensure_found(index=index, doc_type=DefaultType, id=document['document_id'])
                self.client.search_ensure_found(index=index, doc_type=DefaultType, body={
                    "query": {
                        "match": {
                            "payload": document['payload'].split()[-1]
                            }
                        }
                    })

class UserEndlessTaskSet(TaskSet):
    tasks = {UserTaskSet: 1}

class User(ElasticsearchRpcLocust):
    host = '178.128.181.10;178.128.74.142;142.93.16.89'
    min_wait = 100
    max_wait = 1000
    task_set = UserEndlessTaskSet
