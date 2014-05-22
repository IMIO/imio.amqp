# -*- coding: utf-8 -*-
from imio.amqp.tests.base import RabbitMQManager
from imio.amqp.publisher import BasePublisher
import unittest


class TestPublisher(BasePublisher):
    exchange = 'imio.amqp.test'
    batch_interval = 6

    @property
    def stop_timeout(self):
        return getattr(self, '_stop_timeout', 2)

    @stop_timeout.setter
    def stop_timeout(self, value):
        self._stop_timeout = value

    def after_connection_open(self):
        self._connection.add_timeout(self.stop_timeout, self.stop)

    def add_messages(self):
        return ['B', 'B', 'B']

    def get_routing_key(self, message):
        return {'A': 'AA', 'B': 'BB'}.get(message, 'AA')


class TestBasePublisher(unittest.TestCase):

    def setUp(self):
        self._amqp = RabbitMQManager()
        self._amqp.declare_exchange('imio.amqp.test', 'direct', durable='true')

        connection = ('amqp://guest:guest@127.0.0.1:5672/%2F?'
                      'connection_attempts=3&heartbeat_interval=3600')
        self._publisher = TestPublisher(connection, logging=False)

    def tearDown(self):
        self._amqp.delete_queue('imio.amqp.pub1queue')
        self._amqp.delete_queue('imio.amqp.pub2queue')
        self._amqp.cleanup()

    def test_setup_queue(self):
        self._publisher.setup_queue('imio.amqp.pub1queue', 'AA')
        self._publisher.setup_queue('imio.amqp.pub2queue', 'BB')
        self._publisher.start()
        self.assertIn('imio.amqp.pub1queue', self._amqp.queues)
        self.assertIn('imio.amqp.pub2queue', self._amqp.queues)

    def test_single_publisher(self):
        self._publisher.setup_queue('imio.amqp.pub1queue', 'AA')
        self._publisher._messages = ['A', 'A']
        self._publisher.stop_timeout = 4
        self._publisher.start()
        self.assertEqual(2, self._amqp.messages_number('imio.amqp.pub1queue'))

    def test_multiple_publisher(self):
        self._publisher.setup_queue('imio.amqp.pub1queue', 'AA')
        self._publisher.setup_queue('imio.amqp.pub2queue', 'BB')
        self._publisher._messages = ['A', 'B']
        self._publisher.stop_timeout = 3
        self._publisher.start()
        self.assertEqual(1, self._amqp.messages_number('imio.amqp.pub1queue'))
        self.assertEqual(1, self._amqp.messages_number('imio.amqp.pub2queue'))

    def test_add_message(self):
        self._publisher.setup_queue('imio.amqp.pub2queue', 'BB')
        self._publisher.stop_timeout = 10
        self._publisher.start()
        self.assertEqual(3, self._amqp.messages_number('imio.amqp.pub2queue'))
