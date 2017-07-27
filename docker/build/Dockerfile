
FROM girder/girder:latest
MAINTAINER Kitware, Inc. <kitware@kitware.com>

WORKDIR /girder
COPY entrypoint.sh /gi-entrypoint.sh

ENTRYPOINT ["bash", "/gi-entrypoint.sh"]
CMD ["--watch"]

