
FROM girder/girder:latest
MAINTAINER Kitware, Inc. <kitware@kitware.com>

WORKDIR /girder
COPY . /girder/plugins/video

RUN pip install -e .[plugins]
RUN girder-install web --all-plugins

