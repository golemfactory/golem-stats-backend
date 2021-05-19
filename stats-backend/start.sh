#! /bin/bash
# Start yagna service in the background and log it
mkdir -p /golem/work
touch /golem/work/yagna.log
echo "Starting Yagna"
RUST_LOG=error MARKET_DB_CLEANUP_INTERVAL=10min /root/.local/bin/yagna service run > /dev/null 2>&1 &
sleep 5
key=$(/root/.local/bin/yagna app-key create requester)
#export YAGNA_APPKEY="$(yagna app-key list --json | jq -r '.values | map(select(.[0] == "checker")) | .[0][1]')" && npm run ts:low -- --subnet-tag public-beta
cd /stats-backend/ && cd yapapi && git checkout b0.5

#key=$(/root/.local/bin/yagna app-key create requester) && export YAGNA_APPKEY=$key && npm run ts:low -- --subnet-tag public-beta