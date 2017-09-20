
FROM girder/girder:latest
MAINTAINER Kitware, Inc. <kitware@kitware.com>

RUN pip install girder-client ansible
RUN ansible-galaxy install -p /etc/ansible/roles girder.girder

WORKDIR /girder
COPY entrypoint.sh /gi-entrypoint.sh
COPY girder-post-install.yml /girder-post-install.yml
COPY inventory /etc/ansible/hosts

ENTRYPOINT ["bash", "/gi-entrypoint.sh"]
CMD ["--watch"]

