export KEY=$(yagna app-key list --json | jq -r '.values[0][1]')
curl -H "Authorization: Bearer ${KEY}" 127.0.0.1:7465/me
