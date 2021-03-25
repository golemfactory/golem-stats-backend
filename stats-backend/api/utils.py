import requests
import os


def get_stats_data(url):
    user = os.environ.get('STATS_USER')
    password = os.environ.get('STATS_PASSWORD')
    r = requests.get(url, auth=(user, password))
    return r.json()
