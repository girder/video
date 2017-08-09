
Girder Video Plugin Development Environment
===========================================

### Girder Video Dev Env

The development environment for the Girder Video plugin is entirely
self-contained within docker containers.  To take advantage of the environment,
all you need is a recent version of the docker server and client applications;
as well as a recent version of docker-compose.

For reference, below is a sample output of a known-working docker configuration
on Mac OS X.  A similar configuration was also tested on Linux.  NOTE that
docker on a Windows host has not been tested.

```
 $ docker version
```

```
Client:
 Version:      17.06.0-ce
 API version:  1.30
 Go version:   go1.8.3
 Git commit:   02c1d87
 Built:        Fri Jun 23 21:31:53 2017
 OS/Arch:      darwin/amd64

Server:
 Version:      17.06.0-ce
 API version:  1.30 (minimum version 1.12)
 Go version:   go1.8.3
 Git commit:   02c1d87
 Built:        Fri Jun 23 21:51:55 2017
 OS/Arch:      linux/amd64
 Experimental: true
```

For docker compose, you can simply use the latest version from PyPI.  Consider
using a Python virtual environment if you go this route:

```
 $ virtualenv -p python3 env
 $ source env/bin/activate
 $ pip install docker-compose
```

### Creating a new environment

Now, to create the Video plugin development environment entirely from scratch,
enter the docker subdirectory

```
 $ cd docker
```

...and bring up the entire docker container ensemble:

```
 $ docker-compose up -d --build
```

To view the status of the various docker containers, run
```
 $ docker-compose logs -f
```

There's a lot of setup that happens in the beginning, so grab a cup of coffee or
something!  Once log updates stop comming, look around the output for a
"SLEEPING" message from the `pre-build_1` container.  At this point, you should
be ready to browse the test environment on your localhost's port 8080.  To help
estimate the time you should wait, consider that on a stock 2015 Macbook Pro,
building from scratch took around 5-10 minutes.

### Environment details

The development environment comes preset with all the necessary plugins, a
default assetstore, and two users:

```
Privileged User:

name: admin
pass: adminadmin
```

and

```
Nonprivileged User:

name: girder
pass: girder
```

To test the plugin, login (either as one of the precreated accounts, or one of
your own), upload a new video (we used the Creative Commons: Remix video
[here](https://vimeo.com/151666798)).  And test the `PUT /item/{id}/video` route
using the uploaded item's ID.

A job should be scheduled, run without issues, and several new files should be
uploaded to the original item, including metadata about the video, job output
log data, and a new version of the original video transcoded from its source
format to webm with its original audio transcoded to ogg vorbis.

For development, the source directory for the plugin should already be mounted
and configured so that any changes you make would be integrated on-the-fly in
the live development environment.

### Shutting Down/Cleanin Up

Once you are done working, shut down the docker container ensemble with

```
 $ docker-compose down
```

Restarting the containers from this point is very fast, because there is no new
setup work needed:
```
 $ docker-compose up
```

If you need to terminate a container and recreate it, shut it down and then run

```
 $ docker-compose rm [container name]    # or ...
 $ docker-compose rm                     # all containers, also ...
 $ docker-compose rm -f ...              # add '-f' to avoid a prompt
```

Now, when you `docker-compose up`, the container will be recreated.

Some of the containers are built with custom Dockerfile.  If the build files are
changed, their containers will need to be terminated, and their images rebuilt.

```
 $ docker-compose down cont_a cont_b ...
 $ docker-compose rm -f cont_a cont_b ...


 $ docker-compose build cont_a cont_b
 $ docker-compose up -d cont_a cont_b

        or

 $ docker-compose up -d --build cont_a cont_b
```

Finally, as the most destructive option, you can destroy the volumes that are
shared among the containers in the ensemble.  Do this if (for example), you need
to start fresh with an entirely new mongo database, or to update the environment
to a new version of girder.

```
 $ docker-compose down --volumes
 $ docker-compose rm -f
```

Once you bring the ensemble up, again, entirely new volumes will be created.

NOTE: Sometimes, docker-compose fails to properly shut down containers, causing
any volumes in use by it to be undeletable.  If this happens to you, one simple
workaround that I've found is to run the following:

```
 $ docker rm $( docker ps -aq )
```

Then, try to remove the volumes, again.

