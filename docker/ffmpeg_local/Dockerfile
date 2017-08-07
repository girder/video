
FROM ubuntu
MAINTAINER Kitware, Inc. <kitware@kitware.com>
RUN apt-get -yqq update && apt-get -yqq install python3 ffmpeg
COPY convert.py /
ENTRYPOINT ["python3", "/convert.py"]

