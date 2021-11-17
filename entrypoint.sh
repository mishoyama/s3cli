#!/bin/bash

if [ "$1" = 'demon' ]; then
    echo "Demon running..."
    exec top -b > /dev/null
fi

exec "$@"
