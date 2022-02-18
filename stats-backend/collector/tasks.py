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
from .models import Node, NetworkStats, NetworkStatsMax, ProvidersComputing, NetworkAveragePricing, NetworkMedianPricing, NetworkAveragePricingMax, NetworkMedianPricingMax, ProvidersComputingMax, Network, Requestors, requestor_scraper_check
from django.db import connection
from django.db.models import Count, Max, Avg, Min
from api.models import APIHits
from api.serializers import NodeSerializer, NetworkMedianPricingMaxSerializer, NetworkAveragePricingMaxSerializer, ProvidersComputingMaxSerializer, NetworkStatsMaxSerializer, NetworkStatsSerializer, RequestorSerializer
from django.core import serializers
import tempfile
from django.utils import timezone


# jsonmsg = {"user_id": elem, "path": "/src/data/user_avatars/" + elem + ".png"}
# r.lpush("image_classifier", json.dumps(jsonmsg))

pool = redis.ConnectionPool(host='redis', port=6379, db=0)
r = redis.Redis(connection_pool=pool)


@app.task
def save_endpoint_logs_to_db():
    length = r.llen('API')
    # Remove entries in list
    r.delete('API')
    obj, objcreated = APIHits.objects.get_or_create(id=1)
    if objcreated:
        obj.count = length
        obj.save()
    else:
        obj.count = obj.count + length
        obj.save()


@app.task
def requests_served():
    obj = APIHits.objects.get(id=1)
    jsondata = {
        "count": obj.count
    }
    serialized = json.dumps(jsondata)
    r.set("api_requests", serialized)


@app.task
def requestors_to_redis():
    query = Requestors.objects.all().order_by('-tasks_requested')
    serializer = RequestorSerializer(query, many=True)
    data = json.dumps(serializer.data)
    r.set("requestors", data)


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
        'day', 'date')}).values('day').annotate(start=Avg('start'))
    averagecpuh = NetworkAveragePricing.objects.filter(date__gte=start_date).extra(select={'day': connection.ops.date_trunc_sql(
        'day', 'date')}).values('day').annotate(cpuh=Avg('cpuh'))
    averageperh = NetworkAveragePricing.objects.filter(date__gte=start_date).extra(select={'day': connection.ops.date_trunc_sql(
        'day', 'date')}).values('day').annotate(perh=Avg('perh'))
    medianstart = NetworkMedianPricing.objects.filter(date__gte=start_date).extra(select={'day': connection.ops.date_trunc_sql(
        'day', 'date')}).values('day').annotate(start=Min('start'))
    mediancpuh = NetworkMedianPricing.objects.filter(date__gte=start_date).extra(select={'day': connection.ops.date_trunc_sql(
        'day', 'date')}).values('day').annotate(cpuh=Min('cpuh'))
    medianperh = NetworkMedianPricing.objects.filter(date__gte=start_date).extra(select={'day': connection.ops.date_trunc_sql(
        'day', 'date')}).values('day').annotate(perh=Min('perh'))

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
        if obj.data['golem.node.debug.subnet'] != "Thorg":
            pricing_vector = {obj.data['golem.com.usage.vector'][0]: obj.data['golem.com.pricing.model.linear.coeffs']
                              [0], obj.data['golem.com.usage.vector'][1]: obj.data['golem.com.pricing.model.linear.coeffs'][1]}
            if len(str(pricing_vector["golem.usage.duration_sec"])) < 5:
                perhour.append(
                    pricing_vector["golem.usage.duration_sec"])
            else:
                perhour.append(
                    pricing_vector["golem.usage.duration_sec"] * 3600)

                start.append(
                    (obj.data['golem.com.pricing.model.linear.coeffs'][2]))
            if len(str(pricing_vector["golem.usage.cpu_sec"])) < 5:
                cpuhour.append(
                    pricing_vector["golem.usage.cpu_sec"])
            else:
                cpuhour.append(
                    pricing_vector["golem.usage.cpu_sec"] * 3600)

    content = {
        "cpuhour": statistics.mean(cpuhour),
        "perhour": statistics.mean(perhour),
        "start": statistics.mean(start)
    }
    serialized = json.dumps(content)
    NetworkAveragePricing.objects.create(start=statistics.mean(
        start), cpuh=statistics.mean(cpuhour), perh=statistics.mean(perhour))
    r.set("network_average_pricing", serialized)


@ app.task
def network_median_pricing():
    perhour = []
    cpuhour = []
    startprice = []
    data = Node.objects.filter(online=True)
    for obj in data:
        if obj.data['golem.node.debug.subnet'] != "Thorg":
            pricing_vector = {obj.data['golem.com.usage.vector'][0]: obj.data['golem.com.pricing.model.linear.coeffs']
                              [0], obj.data['golem.com.usage.vector'][1]: obj.data['golem.com.pricing.model.linear.coeffs'][1]}
            if len(str(pricing_vector["golem.usage.duration_sec"])) < 5:
                perhour.append(
                    pricing_vector["golem.usage.duration_sec"])
            else:
                perhour.append(
                    pricing_vector["golem.usage.duration_sec"] * 3600)

                startprice.append(
                    (obj.data['golem.com.pricing.model.linear.coeffs'][2]))
            if len(str(pricing_vector["golem.usage.cpu_sec"])) < 5:
                cpuhour.append(
                    pricing_vector["golem.usage.cpu_sec"])
            else:
                cpuhour.append(
                    pricing_vector["golem.usage.cpu_sec"] * 3600)
    content = {
        "cpuhour": statistics.median(cpuhour),
        "perhour": statistics.median(perhour),
        "start": statistics.median(startprice)
    }
    serialized = json.dumps(content)
    NetworkMedianPricing.objects.create(start=statistics.median(
        startprice), cpuh=statistics.median(cpuhour), perh=statistics.median(perhour))
    r.set("network_median_pricing", serialized)


@ app.task
def network_online_to_redis():
    data = Node.objects.filter(online=True)
    serializer = NodeSerializer(data, many=True)
    test = json.dumps(serializer.data)
    r.set("online", test)


@ app.task
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


@ app.task
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


@ app.task
def networkstats_30m():
    now = datetime.now()
    before = now - timedelta(minutes=30)
    data = NetworkStats.objects.filter(
        date__range=(before, now)).order_by('date')
    serializer = NetworkStatsSerializer(data, many=True)
    r.set("stats_30m", json.dumps(serializer.data))


@ app.task
def network_utilization_to_redis():
    end = round(time.time())
    start = end - 21600
    domain = os.environ.get(
        'STATS_URL') + f"api/datasources/proxy/40/api/v1/query_range?query=sum(activity_provider_created%7Bjob%3D~%22community.1%22%7D%20-%20activity_provider_destroyed%7Bjob%3D~%22community.1%22%7D)&start={start}&end={end}&step=30"
    content = get_stats_data(domain)
    if content[1] == 200:
        serialized = json.dumps(content[0])
        r.set("network_utilization", serialized)


@ app.task
def network_node_versions():
    now = round(time.time())
    domain = os.environ.get(
        'STATS_URL') + f'api/datasources/proxy/40/api/v1/query?query=yagna_version_major%7Bjob%3D"community.1"%7D*100%2Byagna_version_minor%7Bjob%3D"community.1"%7D*10%2Byagna_version_patch%7Bjob%3D"community.1"%7D&time={now}'
    data = get_stats_data(domain)
    nodes = data[0]['data']['result']
    for obj in nodes:
        try:
            node = obj['metric']['instance']
            if len(obj['value'][1]) == 2:
                version = "0" + obj['value'][1]
                concatinated = version[0] + "." + version[1] + "." + version[2]
                Node.objects.filter(node_id=node).update(version=concatinated)
            elif len(obj['value'][1]) == 3:
                version = obj['value'][1]
                concatinated = "0." + version[0] + \
                    version[1] + "." + version[2]
                Node.objects.filter(node_id=node).update(version=concatinated)
        except:
            continue


@ app.task
def network_versions_to_redis():
    now = round(time.time())
    domain = os.environ.get(
        'STATS_URL') + f'api/datasources/proxy/40/api/v1/query_range?query=count_values("version"%2C%20yagna_version_major%7Bjob%3D"community.1"%7D*100%2Byagna_version_minor%7Bjob%3D"community.1"%7D*10%2Byagna_version_patch%7Bjob%3D"community.1"%7D)&start={now}&end={now}&step=5'
    content = get_stats_data(domain)
    if content[1] == 200:
        versions_nonsorted = []
        versions = []
        data = content[0]['data']['result']
        # Append to array so we can sort
        for obj in data:
            versions_nonsorted.append(
                {"version": int(obj['metric']['version']), "count": obj['values'][0][1]})
        versions_nonsorted.sort(key=lambda x: x['version'], reverse=False)
        for obj in versions_nonsorted:
            version = str(obj['version'])
            count = obj['count']
            if len(version) == 2:
                concatinated = "0." + version[0] + "." + version[1]
            elif len(version) == 3:
                concatinated = "0." + version[0] + \
                    version[1] + "." + version[2]
            versions.append({
                "version": concatinated,
                "count": count,
            })
        serialized = json.dumps(versions)
        r.set("network_versions", serialized)


@ app.task
def network_earnings_6h_to_redis():
    end = round(time.time())
    # ZKSYNC MAINNET GLM
    domain = os.environ.get(
        'STATS_URL') + f'api/datasources/proxy/40/api/v1/query?query=sum(increase(payment_amount_received%7Bjob%3D~"community.1"%2C%20platform%3D"zksync-mainnet-glm"%7D%5B6h%5D)%2F10%5E9)&time={end}'
    data = get_stats_data(domain)
    if data[1] == 200:
        if data[0]['data']['result']:
            zksync_mainnet_glm = round(
                float(data[0]['data']['result'][0]['value'][1]), 2)
    # ERC20 MAINNET GLM
    domain = os.environ.get(
        'STATS_URL') + f'api/datasources/proxy/40/api/v1/query?query=sum(increase(payment_amount_received%7Bjob%3D~"community.1"%2C%20platform%3D"erc20-mainnet-glm"%7D%5B6h%5D)%2F10%5E9)&time={end}'
    data = get_stats_data(domain)
    if data[1] == 200:
        if data[0]['data']['result']:
            erc20_mainnet_glm = round(
                float(data[0]['data']['result'][0]['value'][1]), 2)
    # ERC20 POLYGON MAINNET GLM
    domain = os.environ.get(
        'STATS_URL') + f'api/datasources/proxy/40/api/v1/query?query=sum(increase(payment_amount_received%7Bjob%3D~"community.1"%2C%20platform%3D"erc20-polygon-glm"%7D%5B6h%5D)%2F10%5E9)&time={end}'
    data = get_stats_data(domain)
    if data[1] == 200:
        if data[0]['data']['result']:
            erc20_polygon_glm = round(
                float(data[0]['data']['result'][0]['value'][1]), 2)
    content = {'total_earnings': zksync_mainnet_glm +
               erc20_mainnet_glm + erc20_polygon_glm}
    serialized = json.dumps(content)
    r.set("network_earnings_6h", serialized)


@ app.task
def network_earnings_24h_to_redis():
    end = round(time.time())
    # ZKSYNC MAINNET GLM
    domain = os.environ.get(
        'STATS_URL') + f'api/datasources/proxy/40/api/v1/query?query=sum(increase(payment_amount_received%7Bjob%3D~"community.1"%2C%20platform%3D"zksync-mainnet-glm"%7D%5B24h%5D)%2F10%5E9)&time={end}'
    data = get_stats_data(domain)
    if data[1] == 200:
        if data[0]['data']['result']:
            zksync_mainnet_glm = round(
                float(data[0]['data']['result'][0]['value'][1]), 2)
    # ERC20 MAINNET GLM
    domain = os.environ.get(
        'STATS_URL') + f'api/datasources/proxy/40/api/v1/query?query=sum(increase(payment_amount_received%7Bjob%3D~"community.1"%2C%20platform%3D"erc20-mainnet-glm"%7D%5B24h%5D)%2F10%5E9)&time={end}'
    data = get_stats_data(domain)
    if data[1] == 200:
        if data[0]['data']['result']:
            erc20_mainnet_glm = round(
                float(data[0]['data']['result'][0]['value'][1]), 2)

    # ERC20 POLYGON MAINNET GLM
    domain = os.environ.get(
        'STATS_URL') + f'api/datasources/proxy/40/api/v1/query?query=sum(increase(payment_amount_received%7Bjob%3D~"community.1"%2C%20platform%3D"erc20-polygon-glm"%7D%5B24h%5D)%2F10%5E9)&time={end}'
    data = get_stats_data(domain)
    if data[1] == 200:
        if data[0]['data']['result']:
            erc20_polygon_glm = round(
                float(data[0]['data']['result'][0]['value'][1]), 2)
    content = {'total_earnings': zksync_mainnet_glm +
               erc20_mainnet_glm + erc20_polygon_glm}
    serialized = json.dumps(content)
    r.set("network_earnings_24h", serialized)


@ app.task
def network_total_earnings():
    end = round(time.time())
    # ZKSYNC MAINNET GLM
    domain = os.environ.get(
        'STATS_URL') + f'api/datasources/proxy/40/api/v1/query?query=sum(increase(payment_amount_received%7Bjob%3D~"community.1"%2C%20platform%3D"zksync-mainnet-glm"%7D%5B1m%5D)%2F10%5E9)&time={end}'
    data = get_stats_data(domain)
    if data[1] == 200:
        if data[0]['data']['result']:
            zksync_mainnet_glm = round(
                float(data[0]['data']['result'][0]['value'][1]), 2)
            if zksync_mainnet_glm > 0:
                db = Network.objects.get(id=1)
                db.total_earnings = db.total_earnings + zksync_mainnet_glm
                db.save()
                content = {'total_earnings': db.total_earnings}
                serialized = json.dumps(content)
                r.set("network_earnings_90d", serialized)
    # ERC20 MAINNET GLM
    domain = os.environ.get(
        'STATS_URL') + f'api/datasources/proxy/40/api/v1/query?query=sum(increase(payment_amount_received%7Bjob%3D~"community.1"%2C%20platform%3D"erc20-mainnet-glm"%7D%5B1m%5D)%2F10%5E9)&time={end}'
    data = get_stats_data(domain)
    if data[1] == 200:
        if data[0]['data']['result']:
            erc20_mainnet_glm = round(
                float(data[0]['data']['result'][0]['value'][1]), 2)
            if erc20_mainnet_glm > 0:
                db = Network.objects.get(id=1)
                db.total_earnings = db.total_earnings + erc20_mainnet_glm
                db.save()
                content = {'total_earnings': db.total_earnings}
                serialized = json.dumps(content)
                r.set("network_earnings_90d", serialized)
    # ERC20 POLYGON MAINNET GLM
    domain = os.environ.get(
        'STATS_URL') + f'api/datasources/proxy/40/api/v1/query?query=sum(increase(payment_amount_received%7Bjob%3D~"community.1"%2C%20platform%3D"erc20-polygon-glm"%7D%5B1m%5D)%2F10%5E9)&time={end}'
    data = get_stats_data(domain)
    if data[1] == 200:
        if data[0]['data']['result']:
            erc20_polygon_glm = round(
                float(data[0]['data']['result'][0]['value'][1]), 2)
            if erc20_polygon_glm > 0:
                db = Network.objects.get(id=1)
                db.total_earnings = db.total_earnings + erc20_polygon_glm
                db.save()
                content = {'total_earnings': db.total_earnings}
                serialized = json.dumps(content)
                r.set("network_earnings_90d", serialized)


@ app.task
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


@ app.task
def providers_average_earnings_to_redis():
    end = round(time.time())
    domain = os.environ.get(
        'STATS_URL') + f'api/datasources/proxy/40/api/v1/query?query=avg(increase(payment_amount_received%7Bjob%3D~"community.1"%2C%20platform%3D"zksync-mainnet-glm"%7D%5B24h%5D)%2F10%5E9)&time={end}'
    data = get_stats_data(domain)
    if data[1] == 200:
        if data[0]['data']['result']:
            zksync_mainnet_glm = round(
                float(data[0]['data']['result'][0]['value'][1]), 4)
    # ERC20 MAINNET GLM
    domain = os.environ.get(
        'STATS_URL') + f'api/datasources/proxy/40/api/v1/query?query=avg(increase(payment_amount_received%7Bjob%3D~"community.1"%2C%20platform%3D"erc20-mainnet-glm"%7D%5B24h%5D)%2F10%5E9)&time={end}'
    data = get_stats_data(domain)
    if data[1] == 200:
        if data[0]['data']['result']:
            erc20_mainnet_glm = round(
                float(data[0]['data']['result'][0]['value'][1]), 4)
    # ERC20 POLYGON MAINNET GLM
    domain = os.environ.get(
        'STATS_URL') + f'api/datasources/proxy/40/api/v1/query?query=avg(increase(payment_amount_received%7Bjob%3D~"community.1"%2C%20platform%3D"erc20-polygon-glm"%7D%5B24h%5D)%2F10%5E9)&time={end}'
    data = get_stats_data(domain)
    if data[1] == 200:
        if data[0]['data']['result']:
            erc20_polygon_glm = round(
                float(data[0]['data']['result'][0]['value'][1]), 4)
    content = {'average_earnings': zksync_mainnet_glm +
               erc20_mainnet_glm + erc20_polygon_glm}
    serialized = json.dumps(content)
    r.set("provider_average_earnings", serialized)


@ app.task
def paid_invoices_1h():
    end = round(time.time())
    domain = os.environ.get(
        'STATS_URL') + f'api/datasources/proxy/40/api/v1/query?query=sum(increase(payment_invoices_provider_paid%7Bjob%3D~"community.1"%7D%5B1h%5D))%2Fsum(increase(payment_invoices_provider_sent%7Bjob%3D~"community.1"%7D%5B1h%5D))&time={end}'
    data = get_stats_data(domain)
    if data[1] == 200:
        if data[0]['data']['result']:
            content = {'percentage_paid': float(data[0]['data']
                       ['result'][0]['value'][1]) * 100}
            serialized = json.dumps(content)
            r.set("paid_invoices_1h", serialized)


@ app.task
def provider_accepted_invoices_1h():
    end = round(time.time())
    domain = os.environ.get(
        'STATS_URL') + f'api/datasources/proxy/40/api/v1/query?query=sum(increase(payment_invoices_provider_accepted%7Bjob%3D~"community.1"%7D%5B1h%5D))%2Fsum(increase(payment_invoices_provider_sent%7Bjob%3D~"community.1"%7D%5B1h%5D))&time={end}'
    data = get_stats_data(domain)
    if data[1] == 200:
        if data[0]['data']['result']:
            content = {'percentage_invoice_accepted': float(data[0]['data']
                       ['result'][0]['value'][1]) * 100}
            serialized = json.dumps(content)
            r.set("provider_accepted_invoice_percentage", serialized)


@ app.task
def online_nodes_computing():
    end = round(time.time())
    providers = Node.objects.filter(online=True)
    for node in providers:
        domain = os.environ.get(
            'STATS_URL') + f'api/datasources/proxy/40/api/v1/query?query=round(increase(activity_provider_created%7Bhostname%3D~%22{node.node_id}%22%2C%20job%3D~%22community.1%22%7D%5B1795s%5D%20offset%2010s)%20-%20increase(activity_provider_destroyed%7Bhostname%3D~%22{node.node_id}%22%2C%20job%3D~%22community.1%22%7D%5B1795s%5D%20offset%205s))&time={end}'
        data = get_stats_data(domain)
        if data[1] == 200:
            if data[0]['data']['result']:
                try:
                    if int(data[0]['data']['result'][0]['value'][1]) >= 1:
                        node.computing_now = True
                        node.save()
                    else:
                        node.computing_now = False
                        node.save()
                except:
                    continue


@ app.task
def node_earnings_total():
    providers = Node.objects.all()
    for user in providers:
        now = round(time.time())
        domain = os.environ.get(
            'STATS_URL') + f'api/datasources/proxy/40/api/v1/query?query=sum(increase(payment_amount_received%7Bhostname%3D~"{user.node_id}"%2C%20platform%3D"zksync-mainnet-glm"%7D%5B10m%5D)%2F10%5E9)&time={now}'
        data = get_stats_data(domain)
        domain2 = os.environ.get(
            'STATS_URL') + f'api/datasources/proxy/40/api/v1/query?query=sum(increase(payment_amount_received%7Bhostname%3D~"{user.node_id}"%2C%20platform%3D"erc20-mainnet-glm"%7D%5B10m%5D)%2F10%5E9)&time={now}'
        data2 = get_stats_data(domain2)
        domain3 = os.environ.get(
            'STATS_URL') + f'api/datasources/proxy/40/api/v1/query?query=sum(increase(payment_amount_received%7Bhostname%3D~"{user.node_id}"%2C%20platform%3D"erc20-polygon-glm"%7D%5B10m%5D)%2F10%5E9)&time={now}'
        data3 = get_stats_data(domain3)
        try:
            zksync_mainnet_glm = round(
                float(data[0]['data']['result'][0]['value'][1]), 2)
            erc20_mainnet_glm = round(
                float(data2[0]['data']['result'][0]['value'][1]), 2)
            erc20_polygon_glm = round(
                float(data3[0]['data']['result'][0]['value'][1]), 2)
            user.earnings_total += zksync_mainnet_glm + \
                erc20_mainnet_glm + erc20_polygon_glm
            user.save(update_fields=['earnings_total'])
        except:
            continue


@ app.task
def market_agreement_termination_reasons():
    end = round(time.time())
    start = round(time.time()) - int(10)
    content = {}
    domain_success = os.environ.get(
        'STATS_URL') + f'api/datasources/proxy/40/api/v1/query?query=sum(increase(market_agreements_provider_terminated_reason%7Bjob%3D"community.1"%2C%20reason%3D"Success"%7D%5B1h%5D))&time={end}'
    data_success = get_stats_data(domain_success)
    if data_success[1] == 200:
        if data_success[0]['data']['result']:
            content['market_agreements_success'] = round(float(
                data_success[0]['data']['result'][0]['value'][1]))
    # Failure
    domain_cancelled = os.environ.get(
        'STATS_URL') + f'api/datasources/proxy/40/api/v1/query?query=sum(increase(market_agreements_provider_terminated_reason%7Bjob%3D"community.1"%2C%20reason%3D"Cancelled"%7D%5B6h%5D))&time={end}'
    data_cancelled = get_stats_data(domain_cancelled)
    if data_cancelled[1] == 200:
        if data_cancelled[0]['data']['result']:
            content['market_agreements_cancelled'] = round(float(
                data_cancelled[0]['data']['result'][0]['value'][1]))
    # Expired
    domain_expired = os.environ.get(
        'STATS_URL') + f'api/datasources/proxy/40/api/v1/query?query=sum(increase(market_agreements_provider_terminated_reason%7Bjob%3D"community.1"%2C%20reason%3D"Expired"%7D%5B6h%5D))&time={end}'
    data_expired = get_stats_data(domain_expired)
    if data_expired[1] == 200:
        if data_expired[0]['data']['result']:
            content['market_agreements_expired'] = round(float(
                data_expired[0]['data']['result'][0]['value'][1]))
    # RequestorUnreachable
    domain_unreachable = os.environ.get(
        'STATS_URL') + f'api/datasources/proxy/40/api/v1/query?query=sum(increase(market_agreements_provider_terminated_reason%7Bjob%3D"community.1"%2C%20reason%3D"RequestorUnreachable"%7D%5B6h%5D))&time={end}'
    data_unreachable = get_stats_data(domain_unreachable)
    if data_unreachable[1] == 200:
        if data_unreachable[0]['data']['result']:
            content['market_agreements_requestorUnreachable'] = round(float(
                data_unreachable[0]['data']['result'][0]['value'][1]))

    # DebitNotesDeadline
    domain_debitdeadline = os.environ.get(
        'STATS_URL') + f'api/datasources/proxy/40/api/v1/query?query=sum(increase(market_agreements_provider_terminated_reason%7Bjob%3D"community.1"%2C%20reason%3D"DebitNotesDeadline"%7D%5B6h%5D))&time={end}'
    data_debitdeadline = get_stats_data(domain_debitdeadline)
    if data_debitdeadline[1] == 200:
        if data_debitdeadline[0]['data']['result']:
            content['market_agreements_debitnoteDeadline'] = round(float(
                data_debitdeadline[0]['data']['result'][0]['value'][1]))
    serialized = json.dumps(content)
    r.set("market_agreement_termination_reasons", serialized)


@ app.task
def requestor_scraper():
    checker, checkcreated = requestor_scraper_check.objects.get_or_create(id=1)
    if checkcreated:
        # No requestors indexed before, we loop back over the last 90 days to init the table with data.
        checker.indexed_before = True
        checker.save()
        now = round(time.time())
        ninetydaysago = round(time.time()) - int(7776000)
        hour = 3600
        while ninetydaysago < now:
            domain = os.environ.get(
                'STATS_URL') + f'api/datasources/proxy/40/api/v1/query?query=increase(market_agreements_requestor_approved%7Bjob%3D"community.1"%7D%5B{hour}s%5D)&time={ninetydaysago+hour}'
            data = get_stats_data(domain)
            ninetydaysago += hour
            if data[1] == 200:
                if data[0]['data']['result']:
                    for node in data[0]['data']['result']:
                        stats_tasks_requested = float(node['value'][1])
                        if stats_tasks_requested > 1:
                            obj, created = Requestors.objects.get_or_create(
                                node_id=node['metric']['instance'])
                            if created:
                                obj.tasks_requested = stats_tasks_requested
                                obj.save()
                            else:
                                obj.tasks_requested = obj.tasks_requested + stats_tasks_requested
                                obj.save()
    else:
        # Already indexed, we check the last 10 seconds.
        now = round(time.time())
        domain = os.environ.get(
            'STATS_URL') + f'api/datasources/proxy/40/api/v1/query?query=increase(market_agreements_requestor_approved%7Bjob%3D"community.1"%7D%5B10s%5D)&time={now}'
        data = get_stats_data(domain)
        if data[1] == 200:
            if data[0]['data']['result']:
                for node in data[0]['data']['result']:
                    stats_tasks_requested = float(node['value'][1])
                    if stats_tasks_requested > 1:
                        obj, created = Requestors.objects.get_or_create(
                            node_id=node['metric']['instance'])
                        if created:
                            obj.tasks_requested = stats_tasks_requested
                            obj.save()
                        else:
                            obj.tasks_requested = obj.tasks_requested + stats_tasks_requested
                            obj.save()


@ app.task
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
            obj.updated_at = timezone.now()
            obj.save(update_fields=['data', 'wallet', 'online', 'updated_at'])
        else:
            obj.data = data
            obj.wallet = wallet
            obj.online = True
            obj.updated_at = timezone.now()
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
                    node.computing_now = False
                    node.updated_at = timezone.now()
                    node.save(update_fields=[
                              'online', 'updated_at', 'computing_now'])
    finally:
        os.remove(path)
