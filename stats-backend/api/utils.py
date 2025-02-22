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


def get_stats_data_v2(query, time_from=None, time_to=None):
    """
    New function for using Grafana's newer API format
    
    Args:
        query: The PromQL query string
        time_from: Start time in Unix timestamp (optional)
        time_to: End time in Unix timestamp (optional)
    """
    service_token = os.environ.get('GRAFANA_SERVICE_TOKEN')
    headers = {'Authorization': f'Bearer {service_token}'}
    
    payload = {
        "queries": [
            {
                "refId": "A",
                "expr": query,
                "instant": True,
                "datasource": {
                    "type": "prometheus",
                    "uid": "dec5owmc8gt8ge"
                }
            }
        ]
    }
    
    # Add time parameters if provided
    if time_from and time_to:
        payload["from"] = str(int(time_from) * 1000)  # Convert to milliseconds
        payload["to"] = str(int(time_to) * 1000)
    
    api_url = f"{os.environ.get('STATS_URL')}api/ds/query"
    
    r = requests.post(api_url, headers=headers, json=payload)
    return [r.json(), r.status_code]