# -*- coding: utf-8 -*-

from imio.amqp.base import AMQPConnector
from imio.amqp.event import ConsumerReadyEvent
from imio.amqp.event import notify

import cPickle


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
        self.treat_message(cPickle.loads(body))
        self.acknowledge_message(basic_deliver.delivery_tag)

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
