# -*- coding: utf-8 -*-

_subscribers = []


class Event(object):
    def __init__(self, context):
        self.context = context


class ConsumerReadyEvent(Event):
    pass


class PublisherReadyEvent(Event):
    pass


class ConnectionOpenedEvent(Event):
    pass


def add_subscriber(event, function):
    _subscribers.append((event, function))


def remove_subscriber(event, function):
    _subscribers.remove((event, function))


def notify(event):
    for subscriber in _subscribers:
        if isinstance(event, subscriber[0]):
            subscriber[1](event)
