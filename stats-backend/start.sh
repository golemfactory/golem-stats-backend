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


echo "Starting Yagna"
yagna service run &
sleep 5
key=$(yagna app-key create requester)
cd /stats-backend/ && cd yapapi && git checkout b0.5