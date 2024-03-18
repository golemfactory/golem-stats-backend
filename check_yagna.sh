#!/bin/sh
set -e # Exit immediately if a command exits with a non-zero status.
set -x # Print commands and their arguments as they are executed.

# Get the Yagna app key
KEY=$(yagna app-key list --json | jq -r '.[0].key')

if [ -z "$KEY" ]; then
    echo "Key is empty, exiting."
    exit 1
fi

# Use the key in the curl command
curl -H "Authorization: Bearer ${KEY}" 127.0.0.1:7465/me

yagna payment status
