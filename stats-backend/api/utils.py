import requests
import os
import aiohttp
import json


def get_stats_data(url):
    service_token = os.environ.get('GRAFANA_SERVICE_TOKEN')
    headers = {'Authorization': f'Bearer {service_token}'}
    r = requests.get(url, headers=headers)
    return [r.json(), r.status_code]


async def get_yastats_data(url):
    service_token = os.environ.get('GRAFANA_SERVICE_TOKEN')
    headers = {'Authorization': f'Bearer {service_token}'}
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as r:
            json_body = await r.json()
            f = json.dumps(json_body)
    return [json_body, r.status]
