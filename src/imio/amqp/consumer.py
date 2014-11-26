# -*- coding: utf-8 -*-

from imio.amqp.base import AMQPConnector
from imio.amqp.event import ConsumerReadyEvent
from imio.amqp.event import notify

import cPickle
import pika


class BaseConsumer(AMQPConnector):

    def treat_message(self, message):
        """Method called during message consumption"""
        raise NotImplementedError('treat_message method must be implemented')

    def start_consuming(self):
        """Begin the consuming of messages"""
        self._log('Begin the consuming of messages')
        self._channel.add_on_cancel_callback(self.on_consumer_cancelled)
        self._consumer_tag = self._channel.basic_consume(self.on_message,
                                                         self.queue)

    def on_consumer_cancelled(self, method_frame):
        """Invoked by pika when RabbitMQ sends a Basic.Cancel for a consumer
        receiving messages."""
        self._log('Consumer was cancelled remotely, shutting down: '
                  '{0!r}'.format(method_frame))
        if self._channel:
            self._channel.close()

    def on_message(self, channel, basic_deliver, properties, body):
        """Consumed a message"""
        self._log('Received message #{0!s} from {0!s}: {0!s}'.format(
            basic_deliver.delivery_tag, properties.app_id, body))
        try:
            self.treat_message(cPickle.loads(body))
            self.acknowledge_message(basic_deliver.delivery_tag)
        except Exception:
            self._log('Error during treatment', type='warning')

    def acknowledge_message(self, delivery_tag):
        """Acknowledged a message"""
        self._log('Acknowledging message {0!s}'.format(delivery_tag))
        self._channel.basic_ack(delivery_tag)

    def stop(self):
        """Stop consuming messages"""
        self._log('Stopping')
        self._closing = True
        if self._channel:
            self._channel.basic_cancel(self.on_cancel, self._consumer_tag)
        # Allow the consumer to cleanly disconnect from RabbitMQ
        self._connection.ioloop.start()
        self._log('Stopped')

    def on_bind(self, response_frame):
        """Called when the queue is ready to consumed messages"""
        self.start_consuming()
        notify(ConsumerReadyEvent(self))

    def on_cancel(self, frame):
        """Stop the connection and the channel"""
        self.close_channel()
        self.close_connection()


class BaseSingleMessageConsumer(BaseConsumer):
    connection_cls = pika.BlockingConnection

    def start(self):
        self._log('Connecting to {0!s}'.format(self._url))
        self._connection = self.connection_cls(pika.URLParameters(self._url))
        self._channel = self._connection.channel()
        self._channel.exchange_declare(exchange=self.exchange,
                                       exchange_type=self.exchange_type,
                                       durable=self.exchange_durable)
        self._channel.queue_declare(self.queue, durable=self.queue_durable,
                                    auto_delete=self.queue_auto_delete)
        self._channel.queue_bind(self.queue, self.exchange,
                                 self.routing_key)

    def get_message(self):
        method_frame, header_frame, body = self._channel.basic_get(self.queue)
        if method_frame:
            self._delivery_tag = method_frame.delivery_tag
            return body

    def acknowledge_message(self):
        self._log('Acknowledging message {0!s}'.format(self._delivery_tag))
        self._channel.basic_ack(self._delivery_tag)

    def stop(self):
        self._log('Stopping')
        self._channel.basic_cancel()
        self._log('Stopped')
