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
YA_NET_RELAY_HOST=yacn2a.dev.golem.network:7477 /root/.local/bin/yagna service run > /dev/null 2>&1 &
sleep 5
key=$(/root/.local/bin/yagna app-key create requester)
#export YAGNA_APPKEY="$(yagna app-key list --json | jq -r '.values | map(select(.[0] == "checker")) | .[0][1]')" && npm run ts:low -- --subnet-tag public-beta
cd /stats-backend/ && cd yapapi && git checkout b0.5

#key=$(/root/.local/bin/yagna app-key create requester) && export YAGNA_APPKEY=$key && npm run ts:low -- --subnet-tag public-beta