FROM ubuntu:latest
MAINTAINER Kitware, Inc. <kitware@kitware.com>

RUN apt-get -yqq update && \
    apt-get -yqq install libjpeg-dev zlib1g-dev libssl-dev \
        python curl git docker.io

RUN curl 'https://bootstrap.pypa.io/get-pip.py' | \
        python && \
    pip install -U pip && \
    pip install virtualenv && \
    virtualenv /env

RUN git clone 'git://github.com/girder/girder_worker' /worker && \
    sh -c '. /env/bin/activate ; pip install /worker ; \
        pip install /worker[girder_io,docker]'

RUN sh -c '. /env/bin/activate \
    girder-worker-config set celery broker "amqp://guest@mq" ; \
    girder-worker-config set girder_worker plugins_enabled docker,girder_io ; \
    girder-worker-config set docker gc False ; \
    girder-worker-config set docker exclude_images mongo,girder,rabbitmq'

COPY entrypoint.sh /gi-entrypoint.sh

ENTRYPOINT ["sh", "/gi-entrypoint.sh"]

