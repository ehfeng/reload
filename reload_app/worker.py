from datetime import datetime
from json import dumps
from time import sleep
from Queue import Queue, Empty
from threading import Lock, Thread
from google.cloud import pubsub_v1
from google.cloud.bigquery import Client as BigQuery


class BigQueryWorker(object):
    def __init__(self, dataset, table, flush_interval=1):
        self.q = Queue(-1)
        self.lock = Lock()
        self.thread = None
        self.flush_interval = flush_interval

        self.dataset = dataset
        self.table = table

    def _start(self):
        if self.thread:
            return

        self.lock.acquire()
        try:
            if self.thread:
                return
            self.thread = Thread(target=self.target)
            self.thread.setDaemon(True)
            self.thread.start()
        finally:
            self.lock.release()
            # import atexit
            # atexit.register(self.graceful_shutdown)

    def queue(self, row):
        self._start()
        self.q.put_nowait({
            'json': row,
            'insertId': row['id'],
        })

    def target(self):
        self.client = BigQuery()
        self.path = '/projects/%s/datasets/%s/tables/%s$%%s/insertAll' % (
            self.client.project, self.dataset, self.table)

        flush_interval = self.flush_interval
        queue = self.q

        while True:
            rows = []
            while True:
                try:
                    rows.append(queue.get(False))
                except Empty:
                    break

            if rows:
                try:
                    self.flush(rows)
                except Exception:
                    import json
                    import sys
                    import traceback
                    json.dump({
                        'exc': traceback.format_exc(),
                        'message': 'Failed to flush buffer',
                    }, sys.stderr)
                    sys.stderr.write('\n')

                for _ in xrange(len(rows)):
                    queue.task_done()

            sleep(flush_interval)

    def flush(self, rows):
        self.client._connection.api_request(
            method='POST',
            path=self.path % datetime.today().strftime('%Y%m%d'),
            data={
                'rows': rows,
                # 'skipInvalidRows': True,
                # 'ignoreUnknownValues': True,
            },
        )

    def graceful_shutdown(self):
        self.lock.acquire()
        try:
            if not self.thread:
                return

            # self.q.put_nowait(self._end)
        finally:
            self.lock.release()


class PubSubWorker(object):
    def __init__(self, project, topic):
        self.project = project
        self.topic = topic
        self.client = None

    def _start(self):
        if self.client is not None:
            return

        batch_settings = pubsub_v1.types.BatchSettings(
            max_bytes=1024*1024*5,
            max_latency=0.05,
            max_messages=1000,
        )
        self.client = pubsub_v1.PublisherClient(batch_settings)
        self.topic_path = self.client.topic_path(self.project, self.topic)

    def publish(self, row):
        self._start()
        self.client.publish(self.topic_path, dumps(row))
