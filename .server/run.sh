#!/usr/bin/env bash

command -v docker >/dev/null 2>&1 || { echo >&2 "This script requires Docker. Ask your interviewer for an alternative"; exit 1; }

localstack_is_running=$(docker ps | grep localstack | wc -l)
if [ "$localstack_is_running" == "1" ]; then
    echo "Localstack is already running."
else
    localstack start -d
fi

echo "Starting event server"
./.server/start.py