
FROM girder/girder:latest
MAINTAINER Kitware, Inc. <kitware@kitware.com>

WORKDIR /girder
COPY entrypoint.sh /gi-entrypoint.sh
COPY girder-post-install.py /girder-post-install.py

ENTRYPOINT ["bash", "/gi-entrypoint.sh"]
CMD ["--watch"]

