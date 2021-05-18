from core.celery import app
from celery import Celery
import json
import subprocess
import os
import statistics
from api.utils import get_stats_data
import time
import redis
from django.db import transaction
from datetime import datetime, timedelta, date
from .models import Node, NetworkStats, NetworkStatsMax, ProvidersComputing, NetworkAveragePricing, NetworkMedianPricing
from django.db import connection
from django.db.models import Count, Max
from api.models import APICounter
from api.serializers import NodeSerializer
from django.core import serializers
import tempfile


# jsonmsg = {"user_id": elem, "path": "/src/data/user_avatars/" + elem + ".png"}
# r.lpush("image_classifier", json.dumps(jsonmsg))

r = redis.Redis(host='redis', port=6379, db=0)


@app.task
def requests_served():
    count = APICounter.objects.all().count()
    jsondata = {
        "count": count
    }
    serialized = json.dumps(jsondata)
    r.set("api_requests", serialized)


@app.task
def stats_snapshot_yesterday():
    start_date = date.today() - timedelta(days=1)
    online = NetworkStats.objects.filter(date__gte=start_date).extra(select={'day': connection.ops.date_trunc_sql(
        'day', 'date')}).values('day').annotate(online=Max('online'))
    cores = NetworkStats.objects.filter(date__gte=start_date).extra(select={'day': connection.ops.date_trunc_sql(
        'day', 'date')}).values('day').annotate(cores=Max("cores"))
    memory = NetworkStats.objects.filter(date__gte=start_date).extra(select={'day': connection.ops.date_trunc_sql(
        'day', 'date')}).values('day').annotate(memory=Max("memory"))
    disk = NetworkStats.objects.filter(date__gte=start_date).extra(select={'day': connection.ops.date_trunc_sql(
        'day', 'date')}).values('day').annotate(disk=Max("disk"))
    test2 = NetworkStatsMax.objects.all()
    for obj in online:
        if obj['day'].date() not in test2:
            online_max = obj['online']
    for obj in cores:
        if obj['day'].date() not in test2:
            cores_max = obj['cores']
    for obj in memory:
        if obj['day'].date() not in test2:
            memory_max = obj['memory']
    for obj in disk:
        if obj['day'].date() not in test2:
            disk_max = obj['disk']

    days = []
    for obj in test2:
        days.append(obj.date.strftime('%Y-%m-%d'))
    if not str(start_date) in str(days):
        print("not in")
        NetworkStatsMax.objects.create(
            online=online_max, cores=cores_max, memory=memory_max, disk=disk_max, date=date.today())


@app.task
def network_average_pricing():
    perhour = []
    cpuhour = []
    start = []
    data = Node.objects.filter(online=True)
    for obj in data:
        if len(str(obj.data['golem.com.pricing.model.linear.coeffs'][0])) < 5:
            perhour.append(
                obj.data['golem.com.pricing.model.linear.coeffs'][0])
        else:
            perhour.append(
                obj.data['golem.com.pricing.model.linear.coeffs'][0] * 3600)

            start.append(
                (obj.data['golem.com.pricing.model.linear.coeffs'][2]))
        if len(str(obj.data['golem.com.pricing.model.linear.coeffs'][1])) < 5:
            cpuhour.append(
                obj.data['golem.com.pricing.model.linear.coeffs'][1])
        else:
            cpuhour.append(
                obj.data['golem.com.pricing.model.linear.coeffs'][1] * 3600)

    content = {
        "cpuhour": statistics.mean(cpuhour),
        "perhour": statistics.mean(perhour),
        "start": statistics.mean(start)
    }
    serialized = json.dumps(content)
    NetworkAveragePricing.objects.create(start=statistics.mean(
        start), cpuh=statistics.mean(cpuhour), perh=statistics.mean(perhour))
    r.set("network_average_pricing", serialized)


@app.task
def network_median_pricing():
    perhour = []
    cpuhour = []
    startprice = []
    data = Node.objects.filter(online=True)
    for obj in data:
        if len(str(obj.data['golem.com.pricing.model.linear.coeffs'][0])) < 5:
            perhour.append(
                obj.data['golem.com.pricing.model.linear.coeffs'][0])
        else:
            perhour.append(
                obj.data['golem.com.pricing.model.linear.coeffs'][0] * 3600)

            startprice.append(
                (obj.data['golem.com.pricing.model.linear.coeffs'][2]))
        if len(str(obj.data['golem.com.pricing.model.linear.coeffs'][1])) < 5:
            cpuhour.append(
                obj.data['golem.com.pricing.model.linear.coeffs'][1])
        else:
            cpuhour.append(
                obj.data['golem.com.pricing.model.linear.coeffs'][1] * 3600)

    content = {
        "cpuhour": statistics.median(cpuhour),
        "perhour": statistics.median(perhour),
        "start": statistics.median(startprice)
    }
    serialized = json.dumps(content)
    NetworkMedianPricing.objects.create(start=statistics.median(
        startprice), cpuh=statistics.median(cpuhour), perh=statistics.median(perhour))
    r.set("network_median_pricing", serialized)


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
    NetworkStats.objects.create(online=len(query), cores=sum(
        threads), memory=sum(memory), disk=sum(disk))
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
def network_earnings_365d_to_redis():
    end = round(time.time())
    start = round(time.time()) - int(10)
    domain = os.environ.get(
        'STATS_URL') + f"api/datasources/proxy/40/api/v1/query_range?query=sum(increase(payment_amount_received%7Bjob%3D~%22community.1%22%7D%5B365d%5D)%2F10%5E9)&start={start}&end={end}&step=1"
    data = get_stats_data(domain)
    content = {'total_earnings': data['data']
               ['result'][0]['values'][-1][1]}
    serialized = json.dumps(content)
    r.set("network_earnings_365d", serialized)


@app.task
def computing_now_to_redis():
    end = round(time.time())
    start = round(time.time()) - int(10)
    domain = os.environ.get(
        'STATS_URL') + f"api/datasources/proxy/40/api/v1/query_range?query=sum(activity_provider_created%7Bjob%3D~%22community.1%22%7D%20-%20activity_provider_destroyed%7Bjob%3D~%22community.1%22%7D)&start={start}&end={end}&step=1"
    data = get_stats_data(domain)
    content = {'computing_now': data['data']['result'][0]['values'][-1][1]}
    ProvidersComputing.objects.create(
        total=data['data']['result'][0]['values'][-1][1])
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
def node_earnings_total():
    providers = Node.objects.all()
    for user in providers:
        now = round(time.time())
        domain = os.environ.get(
            'STATS_URL') + f'api/datasources/proxy/40/api/v1/query?query=sum(increase(payment_amount_received%7Bhostname%3D~"{user.node_id}"%2C%20job%3D~"community.1"%7D%5B8760h%5D)%2F10%5E9)&time={now}'
        data = get_stats_data(domain)
        try:
            content = data['data']['result'][0]['value'][1]
            user.earnings_total = content
            user.save()
        except:
            continue


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
            obj.updated_at = datetime.now()
            obj.save()
        else:
            obj.data = data
            obj.wallet = wallet
            obj.online = True
            obj.updated_at = datetime.now()
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
                    node.updated_at = datetime.now()
                    node.save()
    finally:
        os.remove(path)
