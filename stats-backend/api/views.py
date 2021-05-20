from rest_framework import status
from rest_framework.decorators import api_view
from rest_framework.response import Response
from .utils import get_stats_data, get_yastats_data
import os
import statistics
import time
from collector.models import Node, NetworkStatsMax
from .models import APICounter
from .serializers import NodeSerializer, NetworkStatsMaxSerializer
from django.shortcuts import render
from django.db.models import Count
from django.conf import settings
import asyncio
import redis
import json
import aioredis
from asgiref.sync import sync_to_async


from django.http import JsonResponse, HttpResponse


@sync_to_async
def get_node(yagna_id):
    data = Node.objects.filter(node_id=yagna_id)
    return data


@sync_to_async
def LogEndpoint(endpoint):
    APICounter.objects.create(endpoint=endpoint)


@sync_to_async
def get_node_by_wallet(wallet):
    data = Node.objects.filter(wallet=wallet)
    if data:
        return data
    else:
        return None


async def total_api_calls(request):
    if request.method == 'GET':
        r = await aioredis.create_redis_pool('redis://redis:6379/0')
        content = await r.get("api_requests", encoding='utf-8')
        data = json.loads(content)
        r.close()
        await r.wait_closed()
        return JsonResponse(data, safe=False, json_dumps_params={'indent': 4})
    else:
        return HttpResponse(status=400)


async def median_prices(request):
    await LogEndpoint("Network Median Pricing")
    if request.method == 'GET':
        r = await aioredis.create_redis_pool('redis://redis:6379/0')
        content = await r.get("network_median_pricing", encoding='utf-8')
        data = json.loads(content)
        r.close()
        await r.wait_closed()
        return JsonResponse(data, safe=False, json_dumps_params={'indent': 4})
    else:
        return HttpResponse(status=400)


async def average_pricing(request):
    await LogEndpoint("Network Average Pricing")
    if request.method == 'GET':
        r = await aioredis.create_redis_pool('redis://redis:6379/0')
        content = await r.get("network_average_pricing", encoding='utf-8')
        data = json.loads(content)
        r.close()
        await r.wait_closed()
        return JsonResponse(data, safe=False, json_dumps_params={'indent': 4})
    else:
        return HttpResponse(status=400)


async def statsmax(request):
    """
    Retrieves network stats over time. (Providers, Cores, Memory, Disk)
    """
    await LogEndpoint("Network Historical Stats")
    if request.method == 'GET':
        r = await aioredis.create_redis_pool('redis://redis:6379/0')
        content = await r.get("stats_max", encoding='utf-8')
        data = json.loads(content)
        r.close()
        await r.wait_closed()
        return JsonResponse(data, safe=False, json_dumps_params={'indent': 4})
    else:
        return HttpResponse(status=400)


async def providercomputingmax(request):
    """
    Retrieves providers computing over time. (Highest amount observed during the day)
    """
    await LogEndpoint("Network Historical Computing")
    if request.method == 'GET':
        r = await aioredis.create_redis_pool('redis://redis:6379/0')
        content = await r.get("providers_computing_max", encoding='utf-8')
        data = json.loads(content)
        r.close()
        await r.wait_closed()
        return JsonResponse(data, safe=False, json_dumps_params={'indent': 4})
    else:
        return HttpResponse(status=400)


async def avgpricingmax(request):
    """
    Retrieves Average pricing over time. (Start, CPU/h, Per/h)
    """
    await LogEndpoint("Network Historical Average Pricing")
    if request.method == 'GET':
        r = await aioredis.create_redis_pool('redis://redis:6379/0')
        content = await r.get("pricing_average_max", encoding='utf-8')
        data = json.loads(content)
        r.close()
        await r.wait_closed()
        return JsonResponse(data, safe=False, json_dumps_params={'indent': 4})
    else:
        return HttpResponse(status=400)


async def medianpricingmax(request):
    """
    Retrieves Average pricing over time. (Start, CPU/h, Per/h)
    """
    await LogEndpoint("Network Historical Median Pricing")
    if request.method == 'GET':
        r = await aioredis.create_redis_pool('redis://redis:6379/0')
        content = await r.get("pricing_median_max", encoding='utf-8')
        data = json.loads(content)
        r.close()
        await r.wait_closed()
        return JsonResponse(data, safe=False, json_dumps_params={'indent': 4})
    else:
        return HttpResponse(status=400)


async def online_nodes(request):
    """
    List all online nodes.
    """
    await LogEndpoint("Network Online")
    if request.method == 'GET':
        r = await aioredis.create_redis_pool('redis://redis:6379/0')
        content = await r.get("online", encoding='utf-8')
        data = json.loads(content)
        r.close()
        await r.wait_closed()
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
    return JsonResponse(data)


async def payments_last_n_hours_provider(request, yagna_id, hours):
    await LogEndpoint("Node Earnings")
    now = round(time.time())
    domain = os.environ.get(
        'STATS_URL') + f'api/datasources/proxy/40/api/v1/query?query=sum(increase(payment_amount_received%7Bhostname%3D~"{yagna_id}"%2C%20job%3D~"community.1"%7D%5B{hours}h%5D)%2F10%5E9)&time={now}'
    data = await get_yastats_data(domain)
    content = {'earnings': data['data']
               ['result'][0]['value'][1]}
    return JsonResponse(content)


async def provider_computing(request, yagna_id):
    await LogEndpoint("Node Computing")
    now = round(time.time())
    domain = os.environ.get(
        'STATS_URL') + f'api/datasources/proxy/40/api/v1/query?query=activity_provider_created%7Bhostname%3D~"0xe2462fa49d20324b799b2894bb03fd021489df3a"%2C%20job%3D~"community.1"%7D%20-%20activity_provider_destroyed%7Bhostname%3D~"{yagna_id}"%2C%20job%3D~"community.1"%7D&time={now}'
    data = await get_yastats_data(domain)
    print(data)
    content = {'computing': data['data']
               ['result'][0]['value'][1]}
    return JsonResponse(content)


async def node(request, yagna_id):
    """
    Retrieves data about a specific node.
    """
    await LogEndpoint("Node Detailed")
    if request.method == 'GET':
        data = await get_node(yagna_id)
        serializer = NodeSerializer(data, many=True)
        return JsonResponse(serializer.data, safe=False)
    else:
        return HttpResponse(status=400)


async def node_wallet(request, wallet):
    """
    Returns all the nodes with the specified wallet address.
    """
    await LogEndpoint("Node Operator")
    if request.method == 'GET':
        data = await get_node_by_wallet(wallet.lower())
        print(data)
        if data != None:
            serializer = NodeSerializer(data, many=True)
            return JsonResponse(serializer.data, safe=False)
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
        r = await aioredis.create_redis_pool('redis://redis:6379/0')
        content = await r.get("online_stats")
        data = json.loads(content)
        r.close()
        await r.wait_closed()
        return JsonResponse(data, safe=False)
    else:
        return HttpResponse(status=400)


async def network_utilization(request, start, end):
    """
    Queries the networks utilization from a start date to the end date specified, and returns
    timestamps in ms along with providers computing.
    """
    await LogEndpoint("Network Utilization")
    if request.method == 'GET':
        r = await aioredis.create_redis_pool('redis://redis:6379/0')
        content = await r.get("network_utilization")
        data = json.loads(content)
        r.close()
        await r.wait_closed()
        return JsonResponse(data, safe=False)
    else:
        return HttpResponse(status=400)


async def network_versions(request):
    """
    Queries the networks nodes for their yagna versions
    """
    await LogEndpoint("Network Versions")
    if request.method == 'GET':
        r = await aioredis.create_redis_pool('redis://redis:6379/0')
        content = await r.get("network_versions")
        data = json.loads(content)
        r.close()
        await r.wait_closed()
        return JsonResponse(data, safe=False)
    else:
        return HttpResponse(status=400)


async def providers_computing_currently(request):
    """
    Returns how many providers are currently computing a task.
    """
    await LogEndpoint("Network Computing")
    if request.method == 'GET':
        r = await aioredis.create_redis_pool('redis://redis:6379/0')
        content = await r.get("computing_now")
        data = json.loads(content)
        r.close()
        await r.wait_closed()
        return JsonResponse(data, safe=False)
    else:
        return HttpResponse(status=400)


async def providers_average_earnings(request):
    """
    Returns providers average earnings per task in the last hour.
    """
    await LogEndpoint("Providers Average Earnings")
    if request.method == 'GET':
        r = await aioredis.create_redis_pool('redis://redis:6379/0')
        content = await r.get("provider_average_earnings")
        data = json.loads(content)
        r.close()
        await r.wait_closed()
        return JsonResponse(data, safe=False)
    else:
        return HttpResponse(status=400)


async def network_earnings_24h(request):
    """
    Returns the earnings for the whole network the last n hours.
    """
    await LogEndpoint("Network Earnings 24h")
    if request.method == 'GET':
        r = await aioredis.create_redis_pool('redis://redis:6379/0')
        content = await r.get("network_earnings_24h")
        data = json.loads(content)
        r.close()
        await r.wait_closed()
        return JsonResponse(data, safe=False)
    else:
        return HttpResponse(status=400)


async def network_earnings_365d(request):
    """
    Returns the earnings for the whole network the last n hours.
    """
    await LogEndpoint("Network Earnings 365d")
    if request.method == 'GET':
        r = await aioredis.create_redis_pool('redis://redis:6379/0')
        content = await r.get("network_earnings_365d")
        data = json.loads(content)
        r.close()
        await r.wait_closed()
        return JsonResponse(data, safe=False)
    else:
        return HttpResponse(status=400)


async def network_earnings_6h(request):
    """
    Returns the earnings for the whole network the last n hours.
    """
    await LogEndpoint("Network Earnings 6h")
    if request.method == 'GET':
        r = await aioredis.create_redis_pool('redis://redis:6379/0')
        content = await r.get("network_earnings_6h")
        data = json.loads(content)
        r.close()
        await r.wait_closed()
        return JsonResponse(data, safe=False)
    else:
        return HttpResponse(status=400)
