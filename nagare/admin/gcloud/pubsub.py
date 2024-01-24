# --
# Copyright (c) 2008-2024 Net-ng.
# All rights reserved.
#
# This software is licensed under the BSD License, as described in
# the file LICENSE.txt, which you should have received as part of
# this distribution.
# --

import time
import contextlib

import google

from nagare.admin import command
from nagare.services.gcloud.pubsub import Topic, Subscription


class Commands(command.Commands):
    DESC = 'Google cloud Pub/Sub subcommands'


class Subscribe(command.Command):
    WITH_STARTED_SERVICES = True
    DESC = 'subscribe to a topic'

    def __init__(self, name, dist, **config):
        super(Subscribe, self).__init__(name, dist, **config)
        self.nb = 0

    def set_arguments(self, parser):
        parser.add_argument('-t', '--topic', help='topic service or topic path to subscribe to')
        parser.add_argument('subscription', help='name of the subscription to receive from')

        super(Subscribe, self).set_arguments(parser)

    def handle_request(self, msg):
        print('- {} --------------------'.format(self.nb))

        print('Id: {}'.format(msg.message_id))
        print('Time: {}'.format(msg.publish_time))
        attempt = msg.delivery_attempt
        if attempt is not None:
            print('Attempts: {}'.format(attempt))
        order = msg.ordering_key
        if order:
            print('Order:', order)
        print('Size: {}'.format(msg.size))
        print('Body: {}'.format(msg.data))

        if msg.attributes:
            print('Attributes:')
            padding = len(max(msg.attributes, key=len))
            for k, v in sorted(msg.attributes.items()):
                print(' - {}: {}'.format(k.ljust(padding), v))

        self.nb += 1
        print('')

        msg.ack()

    def run(self, subscription, topic, gcloud_pub_service, services_service):
        if topic:
            if '/' not in topic:
                topic = services_service[topic].path

            subscription = services_service(Subscription, subscription, None, subscription, topic)
        else:
            subscription = services_service[subscription]
            topic = subscription.topic

        print('Listening on <{}>...'.format(subscription))

        with contextlib.suppress(google.api_core.exceptions.AlreadyExists):
            gcloud_pub_service.create_topic({'name': topic})

        with contextlib.suppress(google.api_core.exceptions.AlreadyExists):
            subscription.create()

        subscription.start_consuming(self.handle_request)

        return 0


class Publish(command.Command):
    WITH_STARTED_SERVICES = True
    DESC = 'send data to a topic'

    def set_arguments(self, parser):
        parser.add_argument(
            '-l',
            '--loop',
            action='store_true',
            help='infinite loop sending <data> each 2 seconds',
        )

        parser.add_argument('topic', help='topic service or topic path')
        parser.add_argument('data', help='data to send')

        super(Publish, self).set_arguments(parser)

    @staticmethod
    def run(loop, topic, data, services_service):
        topic = services_service(Topic, topic, None, topic) if '/' in topic else services_service[topic]

        with contextlib.suppress(google.api_core.exceptions.AlreadyExists):
            topic.create()

        try:
            while True:
                topic.publish(data.encode('utf-8')).result()
                time.sleep(1)

                if not loop:
                    break
        except KeyboardInterrupt:
            pass

        return 0
