# -*- coding: utf-8 -*-

from imio.amqp.base import AMQPConnector
from imio.amqp.event import ConnectionOpenedEvent
from imio.amqp.event import notify


def schedule_next_message(self):
    pass


class BaseDispatcher(AMQPConnector):
    logger_name = None
    log_file = None

    def __init__(
        self, consumer_class, publisher_class, amqp_url, logging=True,
    ):
        self._url = amqp_url
        self.logging = logging

        if self.logging is True:
            self._set_logger()
        self._set_publisher(publisher_class)
        self._set_consumer(consumer_class)

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
        self.consumer.publisher = self.publisher
        if self.logging is True:
            self.consumer._logger = self._logger

    def on_connection_open(self, connection):
        self._log('Connection opened')
        notify(ConnectionOpenedEvent(self))
        self._connection.add_on_close_callback(self.on_connection_closed)
        self.consumer._connection = self._connection
        self.publisher._connection = self._connection
        self.consumer.open_channel()
        self.publisher.open_channel()

    def stop(self):
        """Stop the process"""
        self._log('Stopping')
        self.consumer._closing = True
        self.publisher._closing = True
        self.publisher.close_channel()
        if self.consumer._channel:
            self.consumer._channel.basic_cancel(self.consumer.on_cancel,
                                                self.consumer._consumer_tag)
        # Allow the process to cleanly disconnect from RabbitMQ
        self._connection.ioloop.start()
        self._log('Stopped')
