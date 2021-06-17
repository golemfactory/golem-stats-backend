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
from .models import Node, NetworkStats, NetworkStatsMax, ProvidersComputing, NetworkAveragePricing, NetworkMedianPricing, NetworkAveragePricingMax, NetworkMedianPricingMax, ProvidersComputingMax
from django.db import connection
from django.db.models import Count, Max
from api.models import APICounter
from api.serializers import NodeSerializer, NetworkMedianPricingMaxSerializer, NetworkAveragePricingMaxSerializer, ProvidersComputingMaxSerializer, NetworkStatsMaxSerializer, NetworkStatsSerializer
from django.core import serializers
import tempfile


# jsonmsg = {"user_id": elem, "path": "/src/data/user_avatars/" + elem + ".png"}
# r.lpush("image_classifier", json.dumps(jsonmsg))

pool = redis.ConnectionPool(host='redis', port=6379, db=0)
r = redis.Redis(connection_pool=pool)


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
        NetworkStatsMax.objects.create(
            online=online_max, cores=cores_max, memory=memory_max, disk=disk_max, date=date.today())


@app.task
def computing_snapshot_yesterday():
    start_date = date.today() - timedelta(days=1)
    computing = ProvidersComputing.objects.filter(date__gte=start_date).extra(select={'day': connection.ops.date_trunc_sql(
        'day', 'date')}).values('day').annotate(total=Max('total'))
    test2 = ProvidersComputingMax.objects.all()
    for obj in computing:
        if obj['day'].date() not in test2:
            total_max = obj['total']

    days = []
    for obj in test2:
        days.append(obj.date.strftime('%Y-%m-%d'))
    if not str(start_date) in str(days):
        ProvidersComputingMax.objects.create(
            total=total_max, date=date.today())


@app.task
def pricing_snapshot_yesterday():
    start_date = date.today() - timedelta(days=1)
    averagestart = NetworkAveragePricing.objects.filter(date__gte=start_date).extra(select={'day': connection.ops.date_trunc_sql(
        'day', 'date')}).values('day').annotate(start=Max('start'))
    averagecpuh = NetworkAveragePricing.objects.filter(date__gte=start_date).extra(select={'day': connection.ops.date_trunc_sql(
        'day', 'date')}).values('day').annotate(cpuh=Max('cpuh'))
    averageperh = NetworkAveragePricing.objects.filter(date__gte=start_date).extra(select={'day': connection.ops.date_trunc_sql(
        'day', 'date')}).values('day').annotate(perh=Max('perh'))
    medianstart = NetworkMedianPricing.objects.filter(date__gte=start_date).extra(select={'day': connection.ops.date_trunc_sql(
        'day', 'date')}).values('day').annotate(start=Max('start'))
    mediancpuh = NetworkMedianPricing.objects.filter(date__gte=start_date).extra(select={'day': connection.ops.date_trunc_sql(
        'day', 'date')}).values('day').annotate(cpuh=Max('cpuh'))
    medianperh = NetworkMedianPricing.objects.filter(date__gte=start_date).extra(select={'day': connection.ops.date_trunc_sql(
        'day', 'date')}).values('day').annotate(perh=Max('perh'))

    test2 = NetworkAveragePricingMax.objects.all()
    for obj in averagestart:
        if obj['day'].date() not in test2:
            avgstartmax = obj['start']
    for obj in averagecpuh:
        if obj['day'].date() not in test2:
            avgcpuhmax = obj['cpuh']
    for obj in averageperh:
        if obj['day'].date() not in test2:
            avgperhmax = obj['perh']

    test3 = NetworkMedianPricingMax.objects.all()
    for obj in medianstart:
        if obj['day'].date() not in test3:
            medianstartmax = obj['start']
    for obj in mediancpuh:
        if obj['day'].date() not in test3:
            mediancpuhmax = obj['cpuh']
    for obj in medianperh:
        if obj['day'].date() not in test3:
            medianperhmax = obj['perh']

    days = []
    days2 = []
    for obj in test2:
        days.append(obj.date.strftime('%Y-%m-%d'))
    for obj in test3:
        days2.append(obj.date.strftime('%Y-%m-%d'))
    if not str(start_date) in str(days):
        NetworkAveragePricingMax.objects.create(
            start=avgstartmax, cpuh=avgcpuhmax, perh=avgperhmax, date=date.today())
    if not str(start_date) in str(days2):
        NetworkMedianPricingMax.objects.create(
            start=medianstartmax, cpuh=mediancpuhmax, perh=medianperhmax, date=date.today())


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
def max_stats():
    data = ProvidersComputingMax.objects.all()
    serializercomputing = ProvidersComputingMaxSerializer(data, many=True)
    providermax = json.dumps(serializercomputing.data)
    r.set("providers_computing_max", providermax)

    data2 = NetworkAveragePricingMax.objects.all()
    serializeravg = NetworkAveragePricingMaxSerializer(data2, many=True)
    avgmax = json.dumps(serializeravg.data)
    r.set("pricing_average_max", avgmax)

    data3 = NetworkMedianPricingMax.objects.all()
    serializermedian = NetworkMedianPricingMaxSerializer(data3, many=True)
    medianmax = json.dumps(serializermedian.data)
    r.set("pricing_median_max", medianmax)

    data4 = NetworkStatsMax.objects.all()
    serializerstats = NetworkStatsMaxSerializer(data4, many=True)
    statsmax = json.dumps(serializerstats.data)
    r.set("stats_max", statsmax)


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
def networkstats_30m():
    now = datetime.now()
    before = now - timedelta(minutes=30)
    data = NetworkStats.objects.filter(
        date__range=(before, now)).order_by('date')
    serializer = NetworkStatsSerializer(data, many=True)
    r.set("stats_30m", json.dumps(serializer.data))


@app.task
def network_utilization_to_redis():
    end = round(time.time())
    start = end - 21600
    domain = os.environ.get(
        'STATS_URL') + f"api/datasources/proxy/40/api/v1/query_range?query=sum(activity_provider_created%7Bjob%3D~%22community.1%22%7D%20-%20activity_provider_destroyed%7Bjob%3D~%22community.1%22%7D)&start={start}&end={end}&step=30"
    content = get_stats_data(domain)
    if content[1] == 200:
        serialized = json.dumps(content[0])
        r.set("network_utilization", serialized)


@app.task
def network_node_versions():
    now = round(time.time())
    domain = os.environ.get(
        'STATS_URL') + f'api/datasources/proxy/40/api/v1/query?query=yagna_version_major%7Bjob%3D"community.1"%7D*100%2Byagna_version_minor%7Bjob%3D"community.1"%7D*10%2Byagna_version_patch%7Bjob%3D"community.1"%7D&time={now}'
    data = get_stats_data(domain)
    nodes = data[0]['data']['result']
    for obj in nodes:
        try:
            node = obj['metric']['instance']
            version = "0" + obj['value'][1]
            concatinated = version[0] + "." + version[1] + "." + version[2]
            Node.objects.filter(node_id=node).update(version=concatinated)
        except:
            continue


@app.task
def network_versions_to_redis():
    end = round(time.time())
    start = end - 86400
    domain = os.environ.get(
        'STATS_URL') + f'api/datasources/proxy/40/api/v1/query_range?query=count_values("version"%2C%201000%2Byagna_version_major%7Bjob%3D"community.1"%7D*100%2Byagna_version_minor%7Bjob%3D"community.1"%7D*10%2Byagna_version_patch%7Bjob%3D"community.1"%7D)&start={start}&end={end}&step=300'
    content = get_stats_data(domain)
    if content[1] == 200:
        serialized = json.dumps(content)
        r.set("network_versions", serialized)


@app.task
def network_earnings_6h_to_redis():
    end = round(time.time())
    start = round(time.time()) - int(10)
    domain = os.environ.get(
        'STATS_URL') + f"api/datasources/proxy/40/api/v1/query_range?query=sum(increase(payment_amount_received%7Bjob%3D~%22community.1%22%7D%5B6h%5D)%2F10%5E9)&start={start}&end={end}&step=1"
    data = get_stats_data(domain)
    if data[1] == 200:
        if data[0]['data']['result']:
            content = {'total_earnings': data[0]['data']
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
    if data[1] == 200:
        if data[0]['data']['result']:
            content = {'total_earnings': data[0]['data']
                       ['result'][0]['values'][-1][1][0:6]}
            serialized = json.dumps(content)
            r.set("network_earnings_24h", serialized)


@app.task
def network_earnings_90d_to_redis():
    end = round(time.time())
    start = round(time.time()) - int(10)
    domain = os.environ.get(
        'STATS_URL') + f"api/datasources/proxy/40/api/v1/query_range?query=sum(increase(payment_amount_received%7Bjob%3D~%22community.1%22%7D%5B90d%5D)%2F10%5E9)&start={start}&end={end}&step=1"
    data = get_stats_data(domain)
    if data[1] == 200:
        if data[0]['data']['result']:
            content = {'total_earnings': data[0]['data']
                       ['result'][0]['values'][-1][1]}
            serialized = json.dumps(content)
            r.set("network_earnings_90d", serialized)


@app.task
def computing_now_to_redis():
    end = round(time.time())
    start = round(time.time()) - int(10)
    domain = os.environ.get(
        'STATS_URL') + f"api/datasources/proxy/40/api/v1/query_range?query=sum(activity_provider_created%7Bjob%3D~%22community.1%22%7D%20-%20activity_provider_destroyed%7Bjob%3D~%22community.1%22%7D)&start={start}&end={end}&step=1"
    data = get_stats_data(domain)
    if data[1] == 200:
        if data[0]['data']['result']:
            content = {'computing_now': data[0]
                       ['data']['result'][0]['values'][-1][1]}
            ProvidersComputing.objects.create(
                total=data[0]['data']['result'][0]['values'][-1][1])
            serialized = json.dumps(content)
            r.set("computing_now", serialized)


@app.task
def providers_average_earnings_to_redis():
    end = round(time.time())
    start = round(time.time()) - int(10)
    domain = os.environ.get(
        'STATS_URL') + f"api/datasources/proxy/40/api/v1/query_range?query=avg(payment_amount_received%7Bjob%3D~%22community.1%22%7D%2F10%5E9)&start={start}&end={end}&step=1"
    data = get_stats_data(domain)
    if data[1] == 200:
        if data[0]['data']['result']:
            content = {'average_earnings': data[0]['data']
                       ['result'][0]['values'][-1][1][0:5]}
            serialized = json.dumps(content)
            r.set("provider_average_earnings", serialized)


@app.task
def node_earnings_total():
    providers = Node.objects.all()
    for user in providers:
        now = round(time.time())
        domain = os.environ.get(
            'STATS_URL') + f'api/datasources/proxy/40/api/v1/query?query=sum(increase(payment_amount_received%7Bhostname%3D~"{user.node_id}"%7D%5B90d%5D)%2F10%5E9)&time={now}'
        data = get_stats_data(domain)
        try:
            content = data[0]['data']['result'][0]['value'][1]
            user.earnings_total = content
            user.save(update_fields=['earnings_total'])
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
            obj.save(update_fields=['data', 'wallet', 'online', 'updated_at'])
        else:
            obj.data = data
            obj.wallet = wallet
            obj.online = True
            obj.updated_at = datetime.now()
            obj.save(update_fields=['data', 'wallet', 'online', 'updated_at'])
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
                    node.online = False
                    node.updated_at = datetime.now()
                    node.save(update_fields=['online', 'updated_at'])
    finally:
        os.remove(path)
