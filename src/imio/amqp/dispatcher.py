# -*- coding: utf-8 -*-

from imio.amqp.base import AMQPConnector
from imio.amqp.event import ConnectionOpenedEvent
from imio.amqp.event import PublisherReadyEvent
from imio.amqp.event import add_subscriber
from imio.amqp.event import notify
from pika.exceptions import AMQPConnectionError


def schedule_next_message(self):
    pass


def publisher_ready(event):
    event.context.consumer.open_channel()


class BaseDispatcher(AMQPConnector):
    logger_name = None
    log_file = None

    def __init__(self, consumer_class, publisher_class, amqp_url, logging=True):
        self._url = amqp_url
        self.logging = logging
        self._closing = False

        if self.logging is True:
            self._set_logger()
        self._set_publisher(publisher_class)
        self._set_consumer(consumer_class)

        self.publisher.consumer = self.consumer
        self.consumer.publisher = self.publisher

    def _set_publisher(self, cls):
        cls.logger_name = self.logger_name
        cls.log_file = self.log_file
        cls.schedule_next_message = schedule_next_message
        self.publisher = cls(self._url, logging=False)
        if self.logging is True:
            self.publisher._logger = self._logger

    def _set_consumer(self, cls):
        cls.logger_name = self.logger_name
        cls.log_file = self.log_file
        self.consumer = cls(self._url, logging=False)
        if self.logging is True:
            self.consumer._logger = self._logger

    def on_connection_open(self, connection):
        self._log("Connection opened")
        notify(ConnectionOpenedEvent(self))
        self._connection.add_on_close_callback(self.on_connection_closed)
        self.consumer._connection = self._connection
        self.publisher._connection = self._connection
        add_subscriber(PublisherReadyEvent, publisher_ready)
        self.publisher.open_channel()

    def on_connection_closed(self, connection, reply_code, reply_text):
        """Called when the connection to RabbitMQ is closed unexpectedly"""
        if self._closing:
            self._connection.ioloop.stop()
        else:
            self._log(
                "Connection closed, try to reopening: "
                "({0!s}) {1!s}".format(reply_code, reply_text),
                type="warning",
            )
            raise AMQPConnectionError

    def stop(self):
        """Stop the process"""
        self._log("Stopping the dispatcher")
        self.consumer.stop()
        self.publisher.stop()
        self._log("Dispatcher stopped")
