#! /bin/sh
_cputype="$(uname -m)"

case "$_cputype" in
x86_64 | x86-64 | x64 | amd64)
    _cputype=x86_64
    mv /yagna/amd64/yagna /root/.local/bin/yagna
    mv /yagna/amd64/gftp /root/.local/bin/gftp
    ;;
arm64 | aarch64)
    _cputype=aarch64
    mv /yagna/arm64/yagna /root/.local/bin/yagna
    mv /yagna/arm64/gftp /root/.local/bin/gftp
    ;;
*)
    err "invalid cputype: $_cputype"
    ;;
esac
# Start yagna service in the background and log it
mkdir -p /golem/work
touch /golem/work/yagna.log
echo "Starting Yagna"
YAGNA_AUTOCONF_APPKEY=stats /root/.local/bin/yagna service run >/dev/null 2>&1 &
sleep 5
#export YAGNA_APPKEY="$(yagna app-key list --json | jq -r '.values | map(select(.[0] == "checker")) | .[0][1]')" && npm run ts:low -- --subnet-tag public-beta
cd /stats-backend/ && cd yapapi && git checkout b0.5

#key=$(/root/.local/bin/yagna app-key create requester) && export YAGNA_APPKEY=$key && npm run ts:low -- --subnet-tag public-beta
# Define the base file name
BASE_KEY_FILE="key"

# Check if REPLICA is set and non-empty
if [ -n "$REPLICA" ]; then
    KEY_FILE="${BASE_KEY_FILE}-${REPLICA}.json"
else
    KEY_FILE="${BASE_KEY_FILE}.json"
fi

# Path to the key file
KEY_PATH="/${KEY_FILE}"

# Check if the key file exists
if [ -f "$KEY_PATH" ]; then
    echo "Restoring wallet from $KEY_PATH"
    address=$(jq -r '.address' "$KEY_PATH")
    echo "Found wallet with address: 0x${address}"
    yagna id create --from-keystore "$KEY_PATH"
    /root/.local/bin/yagna id update --set-default "0x${address}"
    killall yagna
    sleep 5
    rm $HOME/.local/share/yagna/accounts.json

    YAGNA_AUTOCONF_APPKEY=stats /root/.local/bin/yagna service run >/dev/null 2>&1 &
    sleep 5
    echo "Wallet restored"
fi
