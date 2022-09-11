from rest_framework import status
from rest_framework.decorators import api_view
from rest_framework.response import Response
from .utils import get_stats_data, get_yastats_data
import os
import statistics
import time
from collector.models import Node, NetworkStatsMax, NetworkStats, ProvidersComputing, Benchmark
from .models import APICounter
from .serializers import NodeSerializer, NetworkStatsMaxSerializer, ProvidersComputingMaxSerializer
from django.shortcuts import render
from django.db.models import Count
from django.conf import settings
import asyncio
import redis
import json
import aioredis
from asgiref.sync import sync_to_async
from datetime import datetime
import math

from django.http import JsonResponse, HttpResponse

pool = redis.ConnectionPool(host='redis-cluster-ip-service', port=6379, db=0)
r = redis.Redis(connection_pool=pool)


@sync_to_async
def LogEndpoint(endpoint):
    r.lpush("API", endpoint)


async def total_api_calls(request):
    if request.method == 'GET':
        content = await r.get("api_requests")
        data = json.loads(content)
        return JsonResponse(data, safe=False, json_dumps_params={'indent': 4})
    else:
        return HttpResponse(status=400)


async def median_prices(request):
    await LogEndpoint("Network Median Pricing")
    if request.method == 'GET':
        content = await r.get("network_median_pricing")
        data = json.loads(content)
        return JsonResponse(data, safe=False, json_dumps_params={'indent': 4})
    else:
        return HttpResponse(status=400)


async def average_pricing(request):
    await LogEndpoint("Network Average Pricing")
    if request.method == 'GET':

        content = await r.get("network_average_pricing")
        data = json.loads(content)

        return JsonResponse(data, safe=False, json_dumps_params={'indent': 4})
    else:
        return HttpResponse(status=400)


async def statsmax(request):
    """
    Retrieves network stats over time. (Providers, Cores, Memory, Disk)
    """
    await LogEndpoint("Network Historical Stats")
    if request.method == 'GET':

        content = await r.get("stats_max",)
        data = json.loads(content)

        return JsonResponse(data, safe=False, json_dumps_params={'indent': 4})
    else:
        return HttpResponse(status=400)


async def providercomputingmax(request):
    """
    Retrieves providers computing over time. (Highest amount observed during the day)
    """
    await LogEndpoint("Network Historical Computing")
    if request.method == 'GET':

        content = await r.get("providers_computing_max")
        data = json.loads(content)

        return JsonResponse(data, safe=False, json_dumps_params={'indent': 4})
    else:
        return HttpResponse(status=400)


async def avgpricingmax(request):
    """
    Retrieves Average pricing over time. (Start, CPU/h, Per/h)
    """
    await LogEndpoint("Network Historical Average Pricing")
    if request.method == 'GET':

        content = await r.get("pricing_average_max")
        data = json.loads(content)

        return JsonResponse(data, safe=False, json_dumps_params={'indent': 4})
    else:
        return HttpResponse(status=400)


async def medianpricingmax(request):
    """
    Retrieves Average pricing over time. (Start, CPU/h, Per/h)
    """
    await LogEndpoint("Network Historical Median Pricing")
    if request.method == 'GET':

        content = await r.get("pricing_median_max")
        data = json.loads(content)

        return JsonResponse(data, safe=False, json_dumps_params={'indent': 4})
    else:
        return HttpResponse(status=400)


async def online_nodes(request):
    """
    List all online nodes.
    """
    await LogEndpoint("Network Online")
    if request.method == 'GET':

        content = await r.get("online")
        data = json.loads(content)

        return JsonResponse(data, safe=False, json_dumps_params={'indent': 4})
    else:
        return HttpResponse(status=400)


async def activity_graph_provider(request, yagna_id):
    await LogEndpoint("Node Activity")
    end = round(time.time())
    start = end - 86400
    domain = os.environ.get(
        'STATS_URL') + f'api/datasources/proxy/40/api/v1/query_range?query=sum(changes(activity_provider_usage_0%7Bjob%3D~"community.1"%2C%20instance%3D~"{yagna_id}"%7D%5B60s%5D))&start={start}&end={end}&step=120'
    data = await get_yastats_data(domain)
    if data[1] == 200:
        return JsonResponse(data[0], json_dumps_params={'indent': 4})


async def payments_last_n_hours_provider(request, yagna_id, hours):
    await LogEndpoint("Node Earnings")
    now = round(time.time())
    domain = os.environ.get(
        'STATS_URL') + f'api/datasources/proxy/40/api/v1/query?query=sum(increase(payment_amount_received%7Binstance%3D~"{yagna_id}"%2C%20job%3D~"community.1"%7D%5B{hours}h%5D)%2F10%5E9)&time={now}'
    data = await get_yastats_data(domain)
    if data[1] == 200:
        if data[0]['data']['result']:
            content = {'earnings': data[0]['data']
                       ['result'][0]['value'][1]}
            return JsonResponse(content, json_dumps_params={'indent': 4})
        else:
            content = {'earnings': []}
            return JsonResponse(content, json_dumps_params={'indent': 4})
    else:
        content = {'earnings': []}
        return JsonResponse(content, json_dumps_params={'indent': 4})


async def total_tasks_computed(request, yagna_id):
    await LogEndpoint("Node Tasks Computed")
    now = round(time.time())
    domain = os.environ.get(
        'STATS_URL') + f'api/datasources/proxy/40/api/v1/query?query=sum(increase(market_agreements_provider_terminated_reason%7Binstance%3D~"{yagna_id}"%2C%20reason%3D"Success"%7D%5B90d%5D))&time={now}'
    data = await get_yastats_data(domain)
    if data[1] == 200:
        if data[0]['data']['result']:
            output = int(float(data[0]['data']['result'][0]['value'][1]))
            content = {'tasks_computed_total': output}
            return JsonResponse(content, json_dumps_params={'indent': 4})
        else:
            content = {'tasks_computed_total': []}
            return JsonResponse(content, json_dumps_params={'indent': 4})
    else:
        content = {'tasks_computed_total': []}
        return JsonResponse(content, json_dumps_params={'indent': 4})


async def provider_seconds_computed_total(request, yagna_id):
    await LogEndpoint("Node Seconds Computed")
    now = round(time.time())
    domain = os.environ.get(
        'STATS_URL') + f'api/datasources/proxy/40/api/v1/query?query=sum(increase(activity_provider_usage_1%7Binstance%3D~"{yagna_id}"%7D%5B90d%5D))&time={now}'
    data = await get_yastats_data(domain)
    if data[1] == 200:
        if data[0]['data']['result']:
            output = data[0]['data']['result'][0]['value'][1]
            content = {'seconds_computed': output}
            return JsonResponse(content, json_dumps_params={'indent': 4})
        else:
            content = {'seconds_computed': []}
            return JsonResponse(content, json_dumps_params={'indent': 4})
    else:
        content = {'seconds_computed': []}
        return JsonResponse(content, json_dumps_params={'indent': 4})


async def provider_computing(request, yagna_id):
    await LogEndpoint("Node Computing")
    now = round(time.time())
    domain = os.environ.get(
        'STATS_URL') + f'api/datasources/proxy/40/api/v1/query?query=activity_provider_created%7Binstance%3D~"{yagna_id}"%2C%20job%3D~"community.1"%7D%20-%20activity_provider_destroyed%7Binstance%3D~"{yagna_id}"%2C%20job%3D~"community.1"%7D&time={now}'
    data = await get_yastats_data(domain)
    if data[1] == 200:
        if data[0]['data']['result']:
            content = {'computing': data[0]['data']
                       ['result'][0]['value'][1]}
            return JsonResponse(content, json_dumps_params={'indent': 4})
        else:
            content = {'computing': []}
            return JsonResponse(content, json_dumps_params={'indent': 4})
    else:
        content = {'computing': []}
        return JsonResponse(content, json_dumps_params={'indent': 4})


def node(request, yagna_id):
    """
    Retrieves data about a specific node.
    """
    if request.method == 'GET':
        if yagna_id.startswith("0x"):
            data = Node.objects.filter(node_id=yagna_id)
            if data:
                serializer = NodeSerializer(data, many=True)
                return JsonResponse(serializer.data, safe=False, json_dumps_params={'indent': 4})
            else:
                return HttpResponse(status=404)
        else:
            return HttpResponse(status=404)
    else:
        return HttpResponse(status=400)


def latest_nodes(request):
    """
    Lists all index nodes over time and orders it by the latest node discovered.
    """
    if request.method == 'GET':
        data = Node.objects.all().order_by('-created_at')
        serializer = NodeSerializer(data, many=True)
        return JsonResponse(serializer.data, safe=False, json_dumps_params={'indent': 4})
    else:
        return HttpResponse(status=400)


def latest_nodes_by_number(request, number):
    """
    Lists n amount of the latest nodes indexed.
    """
    if request.method == 'GET':
        data = Node.objects.all().order_by('-created_at')[:number]
        serializer = NodeSerializer(data, many=True)
        return JsonResponse(serializer.data, safe=False, json_dumps_params={'indent': 4})
    else:
        return HttpResponse(status=400)


async def show_endpoint_count(request):
    """
    Lists the amount of times an endpoint has been requested.
    """
    await LogEndpoint("List Endpoint Count")
    if request.method == 'GET':
        endpoint = request.GET['endpoint']
        data = APICounter.objects.filter(endpoint=endpoint).count()
        return JsonResponse(data, safe=False, json_dumps_params={'indent': 4})
    else:
        return HttpResponse(status=400)


def computing_total(request):
    """
    Retrieves data about a specific node.
    """
    if request.method == 'GET':
        data = ProvidersComputing.objects.all().order_by('-total')
        serializer = ProvidersComputingMaxSerializer(data, many=True)
        return JsonResponse(serializer.data, safe=False, json_dumps_params={'indent': 4})
    else:
        return HttpResponse(status=400)


async def stats_30m(request):
    """
    Network stats past 30 minutes.
    """
    await LogEndpoint("Network Online Stats 30m")
    if request.method == 'GET':

        content = await r.get("stats_30m")
        data = json.loads(content)

        return JsonResponse(data, safe=False, json_dumps_params={'indent': 4})
    else:
        return HttpResponse(status=400)


async def node_wallet(request, wallet):
    """
    Returns all the nodes with the specified wallet address.
    """
    await LogEndpoint("Node Operator")
    if request.method == 'GET':
        data = Node.objects.filter(wallet=wallet)
        if data != None:
            serializer = NodeSerializer(data, many=True)
            return JsonResponse(serializer.data, safe=False, json_dumps_params={'indent': 4})
        else:
            return HttpResponse(status=404)

    else:
        return HttpResponse(status=400)


async def general_stats(request):
    """
    List network stats for online nodes.
    """
    await LogEndpoint("Network Online Stats")
    if request.method == 'GET':

        content = await r.get("online_stats")
        data = json.loads(content)

        return JsonResponse(data, safe=False, json_dumps_params={'indent': 4})
    else:
        return HttpResponse(status=400)


async def network_utilization(request):
    """
    Queries the networks utilization from a start date to the end date specified, and returns
    timestamps in ms along with providers computing.
    """
    await LogEndpoint("Network Utilization")
    if request.method == 'GET':

        content = await r.get("network_utilization")
        data = json.loads(content)

        return JsonResponse(data, safe=False, json_dumps_params={'indent': 4})
    else:
        return HttpResponse(status=400)


async def network_versions(request):
    """
    Queries the networks nodes for their yagna versions
    """
    await LogEndpoint("Network Versions")
    if request.method == 'GET':

        content = await r.get("network_versions")
        data = json.loads(content)

        return JsonResponse(data, safe=False, json_dumps_params={'indent': 4})
    else:
        return HttpResponse(status=400)


async def providers_computing_currently(request):
    """
    Returns how many providers are currently computing a task.
    """
    await LogEndpoint("Network Computing")
    if request.method == 'GET':

        content = await r.get("computing_now")
        data = json.loads(content)

        return JsonResponse(data, safe=False, json_dumps_params={'indent': 4})
    else:
        return HttpResponse(status=400)


async def providers_average_earnings(request):
    """
    Returns providers average earnings per task in the last hour.
    """
    await LogEndpoint("Providers Average Earnings")
    if request.method == 'GET':

        content = await r.get("provider_average_earnings")
        data = json.loads(content)

        return JsonResponse(data, safe=False, json_dumps_params={'indent': 4})
    else:
        return HttpResponse(status=400)


async def network_earnings_24h(request):
    """
    Returns the earnings for the whole network the last n hours.
    """
    await LogEndpoint("Network Earnings 24h")
    if request.method == 'GET':

        content = await r.get("network_earnings_24h")
        data = json.loads(content)

        return JsonResponse(data, safe=False, json_dumps_params={'indent': 4})
    else:
        return HttpResponse(status=400)


async def network_earnings_90d(request):
    """
    Returns the earnings for the whole network the last n hours.
    """
    await LogEndpoint("Network Earnings 90d")
    if request.method == 'GET':

        content = await r.get("network_earnings_90d")
        data = json.loads(content)

        return JsonResponse(data, safe=False, json_dumps_params={'indent': 4})
    else:
        return HttpResponse(status=400)


async def network_earnings_6h(request):
    """
    Returns the earnings for the whole network the last n hours.
    """
    await LogEndpoint("Network Earnings 6h")
    if request.method == 'GET':

        content = await r.get("network_earnings_6h")
        data = json.loads(content)

        return JsonResponse(data, safe=False, json_dumps_params={'indent': 4})
    else:
        return HttpResponse(status=400)


async def requestors(request):
    """
    Returns all the requestors seen on the network and the tasks requested amount.
    """
    await LogEndpoint("Requestors")
    if request.method == 'GET':

        content = await r.get("requestors")
        data = json.loads(content)

        return JsonResponse(data, safe=False, json_dumps_params={'indent': 4})
    else:
        return HttpResponse(status=400)


async def market_agreement_termination_reason(request):
    """
    Returns the reasons for market agreements termination.
    """
    await LogEndpoint("Market Agreement Termination")
    if request.method == 'GET':

        content = await r.get("market_agreement_termination_reasons")
        data = json.loads(content)

        return JsonResponse(data, safe=False, json_dumps_params={'indent': 4})
    else:
        return HttpResponse(status=400)


async def paid_invoices_1h(request):
    """
    Returns the percentage of invoices paid during the last hour.
    """
    await LogEndpoint("Paid Invoices 1h")
    if request.method == 'GET':

        content = await r.get("paid_invoices_1h")
        data = json.loads(content)

        return JsonResponse(data, safe=False, json_dumps_params={'indent': 4})
    else:
        return HttpResponse(status=400)


async def provider_invoice_accepted_percentage(request):
    """
    Returns the percentage of invoices accepted by the provider that they have issued to the requestor.
    """
    await LogEndpoint("Provider Invoice Accepted 1h")
    if request.method == 'GET':

        content = await r.get("provider_accepted_invoice_percentage")
        data = json.loads(content)

        return JsonResponse(data, safe=False, json_dumps_params={'indent': 4})
    else:
        return HttpResponse(status=400)


def store_benchmarks(request):
    """
    Store benchmark results
    """
    if request.method == 'POST':
        received_json_data = json.loads(request.body)
        if request.META['HTTP_STATSTOKEN'] == os.getenv("STATS_TOKEN"):
            for obj in received_json_data:
                data = Node.objects.get(node_id=obj['provider_id'])
                benchmark = Benchmark.objects.create(
                    benchmark_score=obj['score'], provider=data, type=request.META['HTTP_BENCHMARKTYPE'])
            return HttpResponse(status=200)
        else:
            return HttpResponse(status=400)
    else:
        return HttpResponse(status=400)
