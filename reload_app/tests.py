import json
from base64 import b64decode
from uuid import UUID, uuid1
from unittest import TestCase
from mock import patch, Mock
from werkzeug.test import Client
from werkzeug.wrappers import BaseResponse

from .app import make_app_from_environ

class AppTests(TestCase):
    def setUp(self):
        super(AppTests, self).setUp()

        patcher = patch('reload_app.app.BigQueryWorker')
        worker_cls = patcher.start()
        self.mock_worker = worker_cls.return_value = Mock()
        self.addCleanup(patcher.stop)

        patcher = patch('google.cloud.pubsub_v1.PublisherClient')
        publisher_cls = patcher.start()
        self.mock_publisher = publisher_cls.return_value = Mock()
        self.addCleanup(patcher.stop)

        patcher = patch('reload_app.app.DogStatsdMetrics')
        dogstatsd_cls = patcher.start()
        self.mock_dogstatsd = dogstatsd_cls.return_value = Mock(spec=['setup', 'increment', 'gauge'])
        self.addCleanup(patcher.stop)

        if not getattr(self, 'client', None):
            app = make_app_from_environ()
            self.client = Client(app, BaseResponse)

    def test_good_input(self):
        sent_data = {
            'url': 'https://sentry.io/',
            'referrer': '/referrer/',
            'user_id': '10',
        }
        resp = self.client.post('/page/', data=json.dumps(sent_data))
        assert resp.status_code == 201
        assert self.mock_worker.queue.call_count == 1
        row = self.mock_worker.queue.call_args[0][0]
        for key in sent_data.keys() + ['id', 'received_at', 'context', 'sent_at']:
            assert key in row

        # /events/ endpoint.
        sent_data.update(
            event_name='assistant.guide_dismissed',
            guide=5,
            step=6,
            unknown_field='something',
        )

        # Make sure events from dev clients aren't accepted.
        sent_data['url'] = 'dev.getsentry.net:8000/'
        resp = self.client.post('/event/', data=json.dumps(sent_data))
        assert resp.status_code == 201
        assert self.mock_publisher.publish.call_count == 0
        sent_data['url'] = 'https://blog.sentry.io/'

        resp = self.client.post('/event/', data=json.dumps(sent_data))
        assert resp.status_code == 201
        assert self.mock_publisher.publish.call_count == 1
        row = json.loads(self.mock_publisher.publish.call_args[1]['data'])
        # Make sure the UUID format is valid.
        UUID(bytes=b64decode(row['uuid']))
        for key in ('timestamp', 'type', 'data'):
            assert key in row
        data = row['data']
        for key in sent_data.keys() + ['received_at', 'context', 'sent_at']:
            if key not in ('event_name', 'unknown_field'):
                assert key in data
        assert 'unknown_field' not in data

    def test_metric_increment(self):
        metric_data = {
            "type": "increment",
            "metric_name": "component.render",
            "tags": {
                "name": "Main",
            }
        }
        resp = self.client.post('/metric/', data=json.dumps(metric_data))
        assert resp.status_code == 201
        assert self.mock_dogstatsd.increment.call_count == 1
        assert self.mock_dogstatsd.increment.call_args[0] == ("component.render", 1)
        assert self.mock_dogstatsd.increment.call_args[1] == {'tags': {'name': 'Main'}}

    def test_metric_gauge(self):
        metric_data = {
            "type": "gauge",
            "value": 123,
            "metric_name": "initial_load",
        }
        resp = self.client.post('/metric/', data=json.dumps(metric_data))
        assert resp.status_code == 201
        assert self.mock_dogstatsd.gauge.call_count == 1
        assert self.mock_dogstatsd.gauge.call_args[0] == ("initial_load", 123)
        assert self.mock_dogstatsd.gauge.call_args[1] == {'tags': {}}

    def test_invalid_metric_name(self):
        metric_data = {
            "type": "increment",
            "value": 123,
            "metric_name": "invalid_metric_name",
        }
        resp = self.client.post('/metric/', data=json.dumps(metric_data))
        assert resp.status_code == 400
        assert resp.data == 'bad request check if valid metric name\n'

    def test_invalid_metric_type(self):
        metric_data = {
            "type": "invalid",
            "value": 123,
            "metric_name": "initial_load",
        }
        resp = self.client.post('/metric/', data=json.dumps(metric_data))
        assert resp.status_code == 400
        assert resp.data == 'bad request check if valid metric type\n'

    def test_invalid_metric_tags(self):
        metric_data = {
            "type": "increment",
            "value": 123,
            "metric_name": "initial_load",
            "tags": {
                "invalid": "Invalid",
            }
        }
        resp = self.client.post('/metric/', data=json.dumps(metric_data))
        assert resp.status_code == 400
        assert resp.data == 'bad request check if valid tag name\n'


    def test_bad_input(self):
        sent_data = {
            'url': '/url/',
            'referrer': '/referrer/',
            'user_id': '10;',
        }
        resp = self.client.post('/page/', data=json.dumps(sent_data))
        assert resp.status_code == 400

        sent_data.update(user_id=10, event_name='click')
        resp = self.client.post('/event/', data=json.dumps(sent_data))
        assert resp.status_code == 400

        sent_data.update(event_name='assistant.guide_dismissed', step='bad type')
        resp = self.client.post('/event/', data=json.dumps(sent_data))
        assert resp.status_code == 400
