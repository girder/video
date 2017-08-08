#! /usr/bin/env sh

. /env/bin/activate
export C_FORCE_ROOT=1

if [ -n "$CELERY_BROKER" ] ; then
    girder-worker-config set celery broker "$CELERY_BROKER"
fi

echo "WAITING FOR DOCKER DAEMON..."
until docker version &> /dev/null ; do
    sleep 5
done

if [ -d /local_images ] ; then
    local_images=`girder-worker-config get docker exclude_images 2> /dev/null`
    cwd=`pwd`
    cd /local_images
    for dir in * ; do
        if [ -f $dir/Dockerfile ] ; then
            echo "BUILDING LOCAL DOCKERFILE: $dir"
            if docker build -t "${dir}_local" "./$dir" ; then
                local_images="$local_images,${dir}_local"
                echo "ADDING LOCAL DOCKERFILE: $dir"
            fi
        fi
    done
    cd "$cwd"

    if [ -n "$local_images" ] ; then
        girder-worker-config set docker exclude_images "$local_images"
    fi
fi

exec girder-worker "$@"

