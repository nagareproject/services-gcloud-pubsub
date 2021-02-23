# Encoding: utf-8

# --
# Copyright (c) 2008-2021 Net-ng.
# All rights reserved.
#
# This software is licensed under the BSD License, as described in
# the file LICENSE.txt, which you should have received as part of
# this distribution.
# --

import sys
import subprocess
from os import path


from setuptools import setup, find_packages

try:
    import stackless  # noqa: F401

    # Under Stackless Python or PyPy, the pre-compiled protobuf and grpcio wheels end with a segfault
    subprocess.check_call([sys.executable] + ' -m pip install --no-binary :all: protobuf grpcio'.split())
except ImportError:
    pass

here = path.normpath(path.dirname(__file__))

with open(path.join(here, 'README.rst')) as long_description:
    LONG_DESCRIPTION = long_description.read()

setup(
    name='nagare-services-gcloud-pubsub',
    author='Net-ng',
    author_email='alain.poirier@net-ng.com',
    description='Google cloud pub/sub service',
    long_description=LONG_DESCRIPTION,
    license='BSD',
    keywords='',
    url='https://github.com/nagareproject/services-gcloud-pubsub',
    packages=find_packages(),
    zip_safe=False,
    setup_requires=['setuptools_scm'],
    use_scm_version=True,
    install_requires=['google-cloud-pubsub>=2.3.0,<3.0.0', 'nagare-server'],
    entry_points='''
        [nagare.commands]
        gcloud = nagare.admin.gcloud.pubsub:Commands

        [nagare.commands.gcloud]
        subscribe = nagare.admin.gcloud.pubsub:Subscribe
        publish = nagare.admin.gcloud.pubsub:Publish

        [nagare.services]
        gcloud.pub = nagare.services.gcloud.pubsub:Publisher
        gcloud.sub = nagare.services.gcloud.pubsub:Subscriber
    '''
)
