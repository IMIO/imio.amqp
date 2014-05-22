# -*- coding: utf-8 -*-
from imio.amqp.tests.base import RabbitMQManager
from imio.amqp.consumer import BaseConsumer
import unittest


class TestConsumer(BaseConsumer):
    queue = 'imio.amqp.testqueue'
    exchange = 'imio.amqp.test'
    routing_key = 'AA'

    def treat_message(self, message):
        if not hasattr(self, '_messages'):
            self._messages = []
        self._messages.append(message)

    def after_connection_open(self):
        self._connection.add_timeout(2, self.stop)


class TestBaseConsumer(unittest.TestCase):

    def setUp(self):
        self._amqp = RabbitMQManager()
        self._amqp.declare_vhost('/test-vhost')
        self._amqp.declare_exchange('imio.amqp.test', 'direct', durable='true')
        self._amqp.declare_queue('imio.amqp.testqueue', durable='true')
        self._amqp.declare_bind('imio.amqp.test', 'imio.amqp.testqueue',
                                routing_key='AA')
        self._amqp.declare_user('john', 'foobar')
        self._amqp.declare_permission('/test-vhost', 'john')

        connection = ('amqp://guest:guest@127.0.0.1:5672/%2F?'
                      'connection_attempts=3&heartbeat_interval=3600')
        self._consumer = TestConsumer(connection, logging=False)

    def tearDown(self):
        self._amqp.cleanup()

    def test_consuming(self):
        self._amqp.publish_message('imio.amqp.test', 'AA', 'foo')
        self._amqp.publish_message('imio.amqp.test', 'AA', 'bar')
        self._consumer.start()
        self.assertEqual(['foo', 'bar'], self._consumer._messages)
