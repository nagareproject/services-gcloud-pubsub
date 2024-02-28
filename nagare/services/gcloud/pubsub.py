# Encoding: utf-8

# --
# Copyright (c) 2008-2024 Net-ng.
# All rights reserved.
#
# This software is licensed under the BSD License, as described in
# the file LICENSE.txt, which you should have received as part of
# this distribution.
# --

"""Provides classes to interact with the Google cloud pub/sub service."""

import os
import platform
import concurrent
import contextlib

import grpc
from google.api_core import exceptions
from google.cloud.pubsub_v1 import PublisherClient, SubscriberClient, types, subscriber
from google.pubsub_v1.services.publisher.transports import PublisherGrpcTransport
from google.pubsub_v1.services.subscriber.transports import SubscriberGrpcTransport

from nagare.server import reference
from nagare.services import proxy, plugin

if (platform.system() == 'Linux') and ('GRPC_POLL_STRATEGY' not in os.environ):
    os.environ['GRPC_POLL_STRATEGY'] = 'epoll1'


@proxy.proxy_to(PublisherClient)
class Publisher(plugin.Plugin):
    CONFIG_SPEC = dict(
        plugin.Plugin.CONFIG_SPEC,
        emulator_host='string(default="")',
        emulator_port='integer(default=8085)',
        max_bytes='integer(default={})'.format(types.BatchSettings().max_bytes),
        max_latency='float(default={})'.format(types.BatchSettings().max_latency),
        max_messages='integer(default={})'.format(types.BatchSettings().max_messages),
        ordering='boolean(default=False)',
        client_options='string(default=None)',
        credentials='string(default=None)',
    )
    proxy_target = None

    def __init__(
        self,
        name,
        dist,
        emulator_host,
        emulator_port,
        max_bytes,
        max_latency,
        max_messages,
        ordering=False,
        client_options=None,
        credentials=None,
        services_service=None,
        **config,
    ):
        services_service(
            plugin.Plugin.__init__,
            self,
            name,
            dist,
            emulator_host=emulator_host,
            emulator_port=emulator_port,
            max_bytes=max_bytes,
            max_latency=max_latency,
            max_messages=max_messages,
            ordering=False,
            client_options=None,
            credentials=None,
            **config,
        )

        batch_settings = types.BatchSettings(max_bytes=max_bytes, max_latency=max_latency, max_messages=max_messages)

        publisher_options = types.PublisherOptions(enable_message_ordering=ordering)

        settings = {}
        if client_options is not None:
            if isinstance(client_options, str):
                client_options = services_service(reference.load_object(client_options)[0])
            settings['client_options'] = client_options

        if emulator_host:
            channel = grpc.insecure_channel('{}:{}'.format(emulator_host, emulator_port))
            transport = PublisherGrpcTransport(channel=channel)
        else:
            transport = None

            if credentials is not None:
                settings['credentials'] = services_service(reference.load_object(credentials)[0])

        self.__class__.proxy_target = PublisherClient(
            batch_settings, publisher_options, transport=transport, **settings
        )


class Topic(plugin.Plugin):
    LOAD_PRIORITY = Publisher.LOAD_PRIORITY + 1
    CONFIG_SPEC = dict(plugin.Plugin.CONFIG_SPEC, path='string', creation='boolean(default=False)')

    def __init__(
        self,
        name,
        dist,
        path,
        creation=False,
        gcloud_pub_service=None,
        services_service=None,
        **config,
    ):
        services_service(
            super(Topic, self).__init__,
            name,
            dist,
            path=path,
            creation=creation,
            **config,
        )

        self.path = path
        self.creation = creation
        self.pub = gcloud_pub_service

    @property
    def subscriptions(self):
        return self.pub.list_topic_subscriptions({'topic': self.path})

    def handle_start(self, app):
        if self.creation:
            with contextlib.suppress(exceptions.AlreadyExists):
                self.create()

    def create(self, **kw):
        return self.pub.create_topic({'name': self.path}, **kw)

    def delete(self, **kw):
        self.pub.delete_topic({'topic': self.path}, **kw)

    def publish(self, data, ordering_key='', *args, **kw):
        return self.pub.publish(self.path, data, ordering_key=ordering_key, *args, **kw)

    def resume_publish(self, ordering_key, *args, **kw):
        self.pub.resume_publish(self.path, ordering_key, *args, **kw)

    def __str__(self):
        return self.path


@proxy.proxy_to(SubscriberClient)
class Subscriber(plugin.Plugin):
    CONFIG_SPEC = dict(
        plugin.Plugin.CONFIG_SPEC,
        emulator_host='string(default="")',
        emulator_port='integer(default=8085)',
        client_options='string(default=None)',
        credentials='string(default=None)',
    )
    proxy_target = None

    def __init__(
        self,
        name,
        dist,
        emulator_host,
        emulator_port,
        client_options=None,
        credentials=None,
        services_service=None,
        **config,
    ):
        services_service(
            super(Subscriber, self).__init__,
            name,
            dist,
            emulator_host=emulator_host,
            emulator_port=emulator_port,
            client_options=client_options,
            credentials=credentials,
            **config,
        )

        settings = {}
        if client_options is not None:
            if isinstance(client_options, str):
                client_options = services_service(reference.load_object(client_options)[0])
            settings['client_options'] = client_options

        if emulator_host:
            channel = grpc.insecure_channel('{}:{}'.format(emulator_host, emulator_port))
            transport = SubscriberGrpcTransport(channel=channel)
        else:
            transport = None

            if credentials is not None:
                settings['credentials'] = services_service(reference.load_object(credentials)[0])

        self.__class__.proxy_target = SubscriberClient(transport=transport, **settings)


class Subscription(plugin.Plugin):
    LOAD_PRIORITY = Subscriber.LOAD_PRIORITY + 1
    CONFIG_SPEC = dict(
        plugin.Plugin.CONFIG_SPEC,
        path='string',
        topic_path='string(default=None)',
        creation='boolean(default=False)',
        pool='integer(default=10)',
    )

    def __init__(
        self,
        name,
        dist,
        path,
        topic_path=None,
        creation=False,
        pool=10,
        gcloud_sub_service=None,
        services_service=None,
        **config,
    ):
        services_service(
            super(Subscription, self).__init__,
            name,
            dist,
            path=path,
            topic_path=topic_path,
            creation=creation,
            pool=pool,
            **config,
        )

        self.path = path
        self.topic = topic_path
        self.creation = creation
        self.pool = pool
        self.sub = gcloud_sub_service

    def handle_start(self, app):
        if self.creation:
            with contextlib.suppress(exceptions.AlreadyExists):
                self.create()

    def create(self, **kw):
        return self.sub.create_subscription({'name': self.path, 'topic': self.topic}, **kw)

    def delete(self, **kw):
        self.sub.delete_subscription({'subscription': self.path}, **kw)

    def acknowledge(self, ack_ids, **kw):
        self.sub.acknowledge({'subscription': self.path, 'ack_ids': ack_ids}, **kw)

    def modify_ack_deadline(self, ack_ids, ack_deadline_seconds, **kw):
        self.sub.modify_ack_deadline(
            {
                'subscription': self.path,
                'ack_ids': ack_ids,
                'ack_deadline_seconds': ack_deadline_seconds,
            },
            **kw,
        )

    def modify_push_config(self, **kw):
        self.sub.modify_push_config({'subscription': self.path}, **kw)

    def create_snapshot(self, name=None, **kw):
        return self.sub.create_snapshot({'subscription': self.path, 'name': name}, **kw)

    def pull(self, max_messages, **kw):
        return self.sub.pull({'subscription': self.path, 'max_messages': max_messages}, **kw)

    def seek(self, snapshot=None, time=None, **kw):
        request = {'subscription': self.path}

        if snapshot is not None:
            request['snapshot'] = snapshot

        if time is not None:
            request['time'] = time

        return self.sub.seek(request, **kw)

    def subscribe(self, callback, *args, **kw):
        executor = concurrent.futures.ThreadPoolExecutor(self.pool)
        scheduler = subscriber.scheduler.ThreadScheduler(executor)

        return self.sub.subscribe(self.path, callback, scheduler=scheduler, *args, **kw)

    def start_consuming(self, callback, *args, **kw):
        future = self.subscribe(callback, *args, **kw)

        try:
            future.result()
        except KeyboardInterrupt:
            future.cancel()

    def __str__(self):
        return self.path
