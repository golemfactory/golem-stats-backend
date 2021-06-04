import requests
import os
import aiohttp
import json


def get_stats_data(url):
    user = os.environ.get('STATS_USER')
    password = os.environ.get('STATS_PASSWORD')
    r = requests.get(url, auth=(user, password))
    return [r.json(), r.status_code]


async def get_yastats_data(url):
    user = os.environ.get('STATS_USER')
    password = os.environ.get('STATS_PASSWORD')
    async with aiohttp.ClientSession() as session:
        async with session.get(url, auth=aiohttp.BasicAuth(user, password)) as r:
            json_body = await r.json()
            f = json.dumps(json_body)
    return [json_body, r.status]
