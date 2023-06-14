from collector.models import Node as NodeV1
from api.serializers import FlatNodeSerializer
from django.shortcuts import render
from .models import Node, Offer
from .serializers import NodeSerializer, OfferSerializer
import redis
import json
import aioredis
import requests

from django.http import JsonResponse, HttpResponse

pool = redis.ConnectionPool(host='redis', port=6379, db=0)
r = redis.Redis(connection_pool=pool)


def globe_data(request):
    # open json file and return data
    with open('/globe_data.geojson') as json_file:
        data = json.load(json_file)
    return JsonResponse(data, safe=False, json_dumps_params={'indent': 4})


async def golem_main_website_index(request):
    if request.method == 'GET':
        pool = aioredis.ConnectionPool.from_url(
            "redis://redis:6379/0", decode_responses=True
        )
        r = aioredis.Redis(connection_pool=pool)

        fetch_blogs = await r.get("v2_index_blog_posts")
        blogs = json.loads(fetch_blogs)

        fetch_network_stats = await r.get("online_stats")
        stats = json.loads(fetch_network_stats)

        fetch_cheapest_providers = await r.get("v2_cheapest_provider")
        cheapest_providers = json.loads(fetch_cheapest_providers)

        pool.disconnect()
        return JsonResponse({'blogs': blogs, 'stats': stats, 'providers': cheapest_providers}, safe=False, json_dumps_params={'indent': 4})
    else:
        return HttpResponse(status=400)


async def node_wallet(request, wallet):
    """
    Returns all the nodes with the specified wallet address.
    """
    if request.method == 'GET':
        data = NodeV1.objects.filter(wallet=wallet)
        if data != None:
            serializer = FlatNodeSerializer(data, many=True)
            return JsonResponse(serializer.data, safe=False, json_dumps_params={'indent': 4})
        else:
            return HttpResponse(status=404)

    else:
        return HttpResponse(status=400)


def node(request, yagna_id):
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


async def network_online(request):
    if request.method == 'GET':
        pool = aioredis.ConnectionPool.from_url(
            "redis://redis:6379/0", decode_responses=True
        )
        r = aioredis.Redis(connection_pool=pool)
        content = await r.get("v2_online")
        data = json.loads(content)
        pool.disconnect()
        return JsonResponse(data, safe=False, json_dumps_params={'indent': 4})
    else:
        return HttpResponse(status=400)


async def network_online_flatmap(request):
    if request.method == 'GET':
        pool = aioredis.ConnectionPool.from_url(
            "redis://redis:6379/0", decode_responses=True
        )
        r = aioredis.Redis(connection_pool=pool)
        content = await r.get("v2_online_flatmap")
        data = json.loads(content)
        pool.disconnect()
        return JsonResponse(data, safe=False, json_dumps_params={'indent': 4})
    else:
        return HttpResponse(status=400)


def cheapest_by_cores(request):
    """ Displays an array of cheapest offers by number of cores that are NOT computing a task right now"""
    cores = {}
    for i in range(256):
        cores[f'cores_{i}'] = []
    req = requests.get(
        "https://api.coingecko.com/api/v3/coins/ethereum/contract/0x7DD9c5Cba05E151C895FDe1CF355C9A1D5DA6429")
    data = req.json()
    price = data['market_data']['current_price']['usd']
    obj = Offer.objects.filter(provider__online=True, runtime="vm",
                               provider__computing_now=False).order_by("monthly_price_glm")
    serializer = OfferSerializer(obj, many=True)
    mainnet_providers = []
    for index, provider in enumerate(serializer.data):
        print(provider['properties'])
        if "golem.com.payment.platform.erc20-mainnet-glm.address" in provider['properties']:
            print("TRUEEEE")
            mainnet_providers.append(provider)
    sorted_pricing_and_specs = sorted(mainnet_providers, key=lambda element: (
        float(element['properties']['golem.inf.cpu.threads']), float(element['monthly_price_glm'])))
    for obj in sorted_pricing_and_specs:
        provider = {}
        provider['name'] = obj['properties']['golem.node.id.name']
        provider['id'] = obj['properties']['id']
        provider['usd_monthly'] = float(
            price) * float(obj['monthly_price_glm'])
        provider['cores'] = float(
            obj['properties']['golem.inf.cpu.threads'])
        provider['memory'] = float(obj['properties']['golem.inf.mem.gib'])
        provider['disk'] = float(
            obj['properties']['golem.inf.storage.gib'])
        provider['glm'] = float(obj['monthly_price_glm'])
        cores_int = int(obj['properties']['golem.inf.cpu.threads'])
        cores[f'cores_{cores_int}'].append(provider)

    for i in range(256):
        cores[f'cores_{i}'] = sorted(
            cores[f'cores_{i}'], key=lambda element: element['usd_monthly'])
    return JsonResponse(cores, safe=False, json_dumps_params={'indent': 4})


async def cheapest_offer(request):
    if request.method == 'GET':
        pool = aioredis.ConnectionPool.from_url(
            "redis://redis:6379/0", decode_responses=True
        )
        r = aioredis.Redis(connection_pool=pool)
        content = await r.get("v2_cheapest_offer")
        data = json.loads(content)
        pool.disconnect()
        return JsonResponse(data, safe=False, json_dumps_params={'indent': 4})
    else:
        return HttpResponse(status=400)
