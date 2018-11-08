import os
import re
import time

from base64 import b64encode
from datetime import datetime
from google.cloud import pubsub_v1
from json import load, dumps
from werkzeug.wrappers import Response
from uuid import uuid1

from .events import VALID_EVENTS
from .metrics import VALID_METRICS
from .metrics.dogstatsd import DogStatsdMetrics
from .raven_client import client
from .router import Router
from .worker import BigQueryWorker
from .utils import format_datetime, ip_from_request

COMMON_FIELDS = ('url', 'referrer', 'title', 'path', 'search', 'anonymous_id', 'user_id')

# Prefix event names to avoid collisions with events from Sentry backend.
EVENT_NAME_TEMPLATE = 'reload.%s'

URL_FILTER_REGEX = r'(https?://)?(localhost|dev.getsentry.net)'


def ok_response():
    return Response(status=201, headers=(
        ('Access-Control-Allow-Origin', '*'),
    ))


def validate_user_id(uid):
    if uid not in (None, 'undefined'):
        try:
            int(uid)
        except ValueError:
            client.captureException()
            return False
    return True


class App(Router):
    routes = {
        '/page/': 'page_view',
        '/event/': 'event',
        '/metric/': 'metric',
    }

    def __init__(self, dataset, table, pubsub_project, pubsub_topic, datadog_prefix, datadog_host, datadog_port):
        super(App, self).__init__()

        self.worker = BigQueryWorker(dataset, table, flush_interval=1)

        batch_settings = pubsub_v1.types.BatchSettings(
            max_bytes=1024*1024*5,
            max_latency=0.05,
            max_messages=1000,
        )
        self.publisher = pubsub_v1.PublisherClient(batch_settings)
        self.topic = self.publisher.topic_path(pubsub_project, pubsub_topic)
        self.datadog_client = DogStatsdMetrics('test-id', prefix=datadog_prefix, host=datadog_host, port=datadog_port)
        self.datadog_client.setup()

    # TODO(adhiraj): Put pageviews in the events table.
    # TODO(adhiraj): This really needs a refactoring.
    def page_view(self, request):
        # Make sure we only get POST requests
        if request.method != 'POST':
            return Response('method not allowed\n', status=405)

        start = datetime.utcnow()

        try:
            data = load(request.stream)
        except Exception:
            return Response('bad request expecting json\n', status=400)

        row = {
            'id': uuid1().hex,
            'received_at': format_datetime(start),
            'context': {
                'ip': ip_from_request(request),
                'user_agent': request.environ.get('HTTP_USER_AGENT'),
            },
        }

        try:
            row['sent_at'] = format_datetime(
                datetime.utcfromtimestamp(int(data['sent_at']) / 1000)
            )
        except Exception:
            # I dunno, maybe KeyError or it's not an integer
            row['sent_at'] = row['received_at']

        for field in COMMON_FIELDS:
            if field == 'user_id' and not validate_user_id(data.get(field)):
                return Response('bad request user id not valid\n', status=400)
            if field == 'url' and re.match(URL_FILTER_REGEX, data.get(field, '')):
                return ok_response()
            try:
                row[field] = data[field]
            except KeyError:
                pass

        self.worker.queue(row)

        return ok_response()

    def event(self, request):
        # Make sure we only get POST requests
        if request.method != 'POST':
            return Response('method not allowed\n', status=405)

        start = datetime.utcnow()

        try:
            data = load(request.stream)
        except Exception:
            return Response('bad request expecting json\n', status=400)

        if data.get('event_name') not in VALID_EVENTS:
            return Response('bad request check if valid event name\n', status=400)

        clean_data = {
            'received_at': format_datetime(start),
            'context': {
                'ip': ip_from_request(request),
                'user_agent': request.environ.get('HTTP_USER_AGENT'),
            },
        }
        try:
            clean_data['sent_at'] = format_datetime(
                datetime.utcfromtimestamp(int(data['sent_at']) / 1000)
            )
        except Exception:
            # I dunno, maybe KeyError or it's not an integer
            clean_data['sent_at'] = clean_data['received_at']

        for field in COMMON_FIELDS:
            if field == 'user_id' and not validate_user_id(data.get(field)):
                return Response('bad request user id not valid\n', status=400)
            if field == 'url' and re.match(URL_FILTER_REGEX, data.get(field, '')):
                return ok_response()
            try:
                clean_data[field] = data[field]
            except KeyError:
                pass

        for field, type_expected in VALID_EVENTS[data['event_name']].items():
            if field not in data:
                continue
            try:
                type_expected(data[field])
            except ValueError:
                client.captureException()
                return Response('bad request maybe check field type\n', status=400)

            type_received = type(data[field])
            if type_expected != type_received and not (
                    type_received is unicode and type_expected is str):
                client.captureMessage(
                    'expected %s, received %s for field %s of event %s' % (
                        type_expected,
                        type_received,
                        field,
                        data['event_name'],
                    ),
                    level='warning',
                )
            clean_data[field] = data[field]

        # Conforms to super-big-data.analytics.events schema.
        row = {
            'uuid': b64encode(uuid1().bytes),
            'timestamp': time.time(),
            'type': EVENT_NAME_TEMPLATE % data['event_name'],
            'data': clean_data,
        }
        self.publisher.publish(self.topic, data=dumps(row))

        return ok_response()

    def metric(self, request):
        # Make sure we only get POST requests
        if request.method != 'POST':
            return Response('method not allowed\n', status=405)

        try:
            data = load(request.stream)
        except Exception:
            return Response('bad request expecting json\n', status=400)

        metric_name = data.get('metric_name')
        metric_type = data.get('type')
        tags = data.get('tags', {})

        # allowed list of metric names
        if metric_name not in VALID_METRICS:
            return Response('bad request check if valid metric name\n', status=400)

        # validate tags
        for tag in tags.keys():
            if tag not in VALID_METRICS[metric_name]:
                return Response('bad request check if valid tag name\n', status=400)

        try:
            value = data['value']
        except KeyError as e:
            # Allow default value for increment only
            if metric_type == 'increment':
                value = 1
            else:
                return Response('bad request check if valid value for metric\n', status=400)

        try:
            getattr(self.datadog_client, metric_type)(metric_name, value, tags=tags)
        except AttributeError:
            return Response('bad request check if valid metric type\n', status=400)
        except Exception as e:
            return Response('failed request to metrics server', status=400)

        return ok_response()


def make_app_from_environ():
    from werkzeug.contrib.fixers import ProxyFix
    from raven.middleware import Sentry
    app = App(
        dataset=os.environ.get('BIGQUERY_DATASET', 'reload'),
        table=os.environ.get('BIGQUERY_TABLE', 'page'),
        pubsub_project=os.environ.get('PUBSUB_PROJECT', 'internal-sentry'),
        pubsub_topic=os.environ.get('PUBSUB_TOPIC', 'analytics-events'),
        datadog_prefix=os.environ.get('DATADOG_PREFIX', ''),
        datadog_host=os.environ.get('DATADOG_HOST', '127.0.0.1'),
        datadog_port=os.environ.get('DATADOG_PORT', 8125),
    )
    return ProxyFix(Sentry(app, client))
