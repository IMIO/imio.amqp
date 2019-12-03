# -*- coding: utf-8 -*-
from imio.amqp.consumer import BaseConsumer
from imio.amqp.dispatcher import BaseDispatcher
from imio.amqp.event import ConnectionOpenedEvent
from imio.amqp.event import add_subscriber
from imio.amqp.event import _subscribers
from imio.amqp.publisher import BasePublisher
from imio.amqp.tests.base import RabbitMQManager
from pika.exceptions import ChannelClosed

import unittest


class TestConsumer(BaseConsumer):
    queue = "imio.amqp.conqueue"
    exchange = "imio.amqp.test"
    routing_key = "key"

    def treat_message(self, message):
        self.publisher.publish(message)


class TestPublisher(BasePublisher):
    exchange = "imio.amqp.test"

    def get_routing_key(self, message):
        return {"A": "AA", "B": "BB"}[message]


class TestDispatcher(BaseDispatcher):
    after_connection = "stop"

    @property
    def stop_timeout(self):
        return getattr(self, "_stop_timeout", 4)

    @stop_timeout.setter
    def stop_timeout(self, value):
        self._stop_timeout = value

    def publish_base(self):
        self._amqp.publish_message("imio.amqp.test", "key", "A")
        self._amqp.publish_message("imio.amqp.test", "key", "B")
        self._amqp.publish_message("imio.amqp.test", "key", "A")
        self._amqp.publish_message("imio.amqp.test", "key", "B")
        self._amqp.publish_message("imio.amqp.test", "key", "B")


def after_connection_open(event):
    event.context._connection.add_timeout(
        event.context.stop_timeout, event.context.stop
    )


def after_connection_open_publish(event):
    event.context._connection.add_timeout(3, event.context.publish_base)


class TestBaseDispatcher(unittest.TestCase):
    def setUp(self):
        self._amqp = RabbitMQManager()
        self._amqp.declare_exchange("imio.amqp.test", "direct", durable="true")
        self._amqp.declare_queue("imio.amqp.conqueue", durable="true")
        self._amqp.declare_bind(
            "imio.amqp.test", "imio.amqp.conqueue", routing_key="key"
        )

        connection = (
            "amqp://guest:guest@127.0.0.1:5672/%2F?"
            "connection_attempts=3&heartbeat_interval=3600"
        )
        self._dispatcher = TestDispatcher(
            TestConsumer, TestPublisher, connection, logging=False
        )
        self._dispatcher._amqp = self._amqp
        add_subscriber(ConnectionOpenedEvent, after_connection_open)

    def tearDown(self):
        _subscribers[:] = []
        self._amqp.delete_queue("imio.amqp.pub1queue")
        self._amqp.delete_queue("imio.amqp.pub2queue")
        self._amqp.cleanup()

    def test_base_dispatcher(self):
        self._dispatcher.publisher.setup_queue("imio.amqp.pub1queue", "AA")
        self._dispatcher.publisher.setup_queue("imio.amqp.pub2queue", "BB")
        self._dispatcher.stop_timeout = 8
        add_subscriber(ConnectionOpenedEvent, after_connection_open_publish)

        self.assertRaises(ChannelClosed, self._dispatcher.start)
        self.assertEqual(2, self._amqp.messages_number("imio.amqp.pub1queue"))
        self.assertEqual(3, self._amqp.messages_number("imio.amqp.pub2queue"))

    def test_dispatcher_existing_messages(self):
        self._dispatcher.publisher.setup_queue("imio.amqp.pub1queue", "AA")
        self._amqp.publish_message("imio.amqp.test", "key", "A")
        self._amqp.publish_message("imio.amqp.test", "key", "A")

        self.assertRaises(ChannelClosed, self._dispatcher.start)
        self.assertEqual(2, self._amqp.messages_number("imio.amqp.pub1queue"))

    def test_publish_on_missing_queue(self):
        self._dispatcher.publisher.setup_queue("imio.amqp.pub1queue", "AA")
        self._dispatcher.stop_timeout = 8
        add_subscriber(ConnectionOpenedEvent, after_connection_open_publish)

        self.assertRaises(ChannelClosed, self._dispatcher.start)
        self.assertEqual(2, self._amqp.messages_number("imio.amqp.pub1queue"))
        self.assertEqual(3, self._amqp.messages_number("imio.amqp.conqueue"))
