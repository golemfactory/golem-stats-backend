from core.celery import app
from celery import Celery
import json
import subprocess
import os
from api.utils import get_stats_data
import time
import redis
from django.db import transaction
from .models import Node
from api.serializers import NodeSerializer
from django.core import serializers
import tempfile


# jsonmsg = {"user_id": elem, "path": "/src/data/user_avatars/" + elem + ".png"}
# r.lpush("image_classifier", json.dumps(jsonmsg))

r = redis.Redis(host='redis', port=6379, db=0)


@app.task
def network_online_to_redis():
    data = Node.objects.filter(online=True)
    serializer = NodeSerializer(data, many=True)
    test = json.dumps(serializer.data)
    r.set("online", test)


@app.task
def network_stats_to_redis():
    cores = []
    threads = []
    memory = []
    disk = []
    query = Node.objects.filter(online=True)
    for obj in query:
        cores.append(obj.data['golem.inf.cpu.cores'])
        threads.append(obj.data['golem.inf.cpu.threads'])
        memory.append(obj.data['golem.inf.mem.gib'])
        disk.append(obj.data['golem.inf.storage.gib'])
    content = {'online': len(query), 'cores': sum(
        cores), 'threads': sum(threads), 'memory': sum(memory), 'disk': sum(disk)}
    serialized = json.dumps(content)
    r.set("online_stats", serialized)


@app.task
def network_utilization_to_redis():
    end = round(time.time())
    start = end - 21600
    domain = os.environ.get(
        'STATS_URL') + f"api/datasources/proxy/40/api/v1/query_range?query=sum(activity_provider_created%7Bjob%3D~%22community.1%22%7D%20-%20activity_provider_destroyed%7Bjob%3D~%22community.1%22%7D)&start={start}&end={end}&step=30"
    content = get_stats_data(domain)
    serialized = json.dumps(content)
    r.set("network_utilization", serialized)


@app.task
def network_versions_to_redis():
    end = round(time.time())
    start = end - 86400
    domain = os.environ.get(
        'STATS_URL') + f'api/datasources/proxy/40/api/v1/query_range?query=count_values("version"%2C%201000%2Byagna_version_major%7Bjob%3D"community.1"%7D*100%2Byagna_version_minor%7Bjob%3D"community.1"%7D*10%2Byagna_version_patch%7Bjob%3D"community.1"%7D)&start={start}&end={end}&step=300'
    content = get_stats_data(domain)
    serialized = json.dumps(content)
    r.set("network_versions", serialized)


@app.task
def network_earnings_6h_to_redis():
    end = round(time.time())
    start = round(time.time()) - int(10)
    domain = os.environ.get(
        'STATS_URL') + f"api/datasources/proxy/40/api/v1/query_range?query=sum(increase(payment_amount_received%7Bjob%3D~%22community.1%22%7D%5B6h%5D)%2F10%5E9)&start={start}&end={end}&step=1"
    data = get_stats_data(domain)
    content = {'total_earnings': data['data']
               ['result'][0]['values'][-1][1][0:6]}
    serialized = json.dumps(content)
    r.set("network_earnings_6h", serialized)


@app.task
def network_earnings_24h_to_redis():
    end = round(time.time())
    start = round(time.time()) - int(10)
    domain = os.environ.get(
        'STATS_URL') + f"api/datasources/proxy/40/api/v1/query_range?query=sum(increase(payment_amount_received%7Bjob%3D~%22community.1%22%7D%5B24h%5D)%2F10%5E9)&start={start}&end={end}&step=1"
    data = get_stats_data(domain)
    content = {'total_earnings': data['data']
               ['result'][0]['values'][-1][1][0:6]}
    serialized = json.dumps(content)
    r.set("network_earnings_24h", serialized)


@app.task
def computing_now_to_redis():
    end = round(time.time())
    start = round(time.time()) - int(10)
    domain = os.environ.get(
        'STATS_URL') + f"api/datasources/proxy/40/api/v1/query_range?query=sum(activity_provider_created%7Bjob%3D~%22community.1%22%7D%20-%20activity_provider_destroyed%7Bjob%3D~%22community.1%22%7D)&start={start}&end={end}&step=1"
    data = get_stats_data(domain)
    content = {'computing_now': data['data']['result'][0]['values'][-1][1]}
    serialized = json.dumps(content)
    r.set("computing_now", serialized)


@app.task
def providers_average_earnings_to_redis():
    end = round(time.time())
    start = round(time.time()) - int(10)
    domain = os.environ.get(
        'STATS_URL') + f"api/datasources/proxy/40/api/v1/query_range?query=avg(payment_amount_received%7Bjob%3D~%22community.1%22%7D%2F10%5E9)&start={start}&end={end}&step=1"
    data = get_stats_data(domain)
    content = {'average_earnings': data['data']
               ['result'][0]['values'][-1][1][0:5]}
    serialized = json.dumps(content)
    r.set("provider_average_earnings", serialized)


@app.task
def offer_scraper():
    os.chdir("/stats-backend/yapapi/examples/low-level-api")
    with open('data.config') as f:
        for line in f:
            command = line
    proc = subprocess.Popen(command, shell=True)
    proc.wait()
    content = r.get("offers")
    serialized = json.loads(content)
    for line in serialized:
        data = json.loads(line)
        provider = data['id']
        wallet = data['wallet']
        obj, created = Node.objects.get_or_create(node_id=provider)
        if created:
            obj.data = data
            obj.wallet = wallet
            obj.online = True
            obj.save()
        else:
            obj.data = data
            obj.wallet = wallet
            obj.online = True
            obj.save()
    # Find offline providers
    str1 = ''.join(serialized)
    fd, path = tempfile.mkstemp()
    try:
        with os.fdopen(fd, 'w') as tmp:
            # do stuff with temp file
            tmp.write(str1)
            online_nodes = Node.objects.filter(online=True)
            for node in online_nodes:
                if not node.node_id in str1:
                    print("not found", node.node_id)
                    node.online = False
                    node.save()
    finally:
        os.remove(path)
