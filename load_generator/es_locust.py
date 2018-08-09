#!/usr/bin/env python
import time
import elasticsearch

from locust import Locust, TaskSet, events, task

class ElasticsearchClient:
    def __init__(self, hosts):
        self.client = elasticsearch.Elasticsearch(hosts)

    def __getattr__(self, name):
        func = self.client.__getattribute__(name)
        def wrapper(*args, **kwargs):
            return self._wrap_client_call('{}-{}'.format(name, kwargs['index']), lambda: func(*args, **kwargs))
        return wrapper

    def get_ensure_found(self, *args, **kwargs):
        return self._wrap_client_call(
                'get_ensure_found-{}'.format(kwargs['index']), 
                lambda: self.get(*args, **kwargs), 
                lambda r: 'found' in r and r['found'] is True)

    def search_ensure_found(self, *args, **kwargs):
        return self._wrap_client_call(
                'search_ensure_found-{}'.format(kwargs['index']),
                lambda: self.search(*args, **kwargs),
                lambda r: r['hits']['total'] == 1)

    def _wrap_client_call(self, name, call, success_filter=None):
        failed = False
        exception = None
        start_time = time.time()
        try:
            response = call()
            return response
        except ConnectionRefusedError as e:
            failed = True
            exception = e
        finally:
            total_time = int((time.time() - start_time) * 1000)
            if not failed and (success_filter is None or success_filter(response)):
                events.request_success.fire(request_type="elasticsearch", name=name, response_time=total_time, response_length=len(str(response)))
            else:
                events.request_failure.fire(request_type="elasticsearch", name=name, response_time=total_time, exception=exception)


class ElasticsearchRpcLocust(Locust):
    def __init__(self, *args, **kwargs):
        super(ElasticsearchRpcLocust, self).__init__(*args, **kwargs)
        self.client = ElasticsearchClient(self.host.split(';'))
