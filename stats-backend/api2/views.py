from collector.models import Node as NodeV1
from api.serializers import FlatNodeSerializer
from django.shortcuts import render
from .models import Node, Offer, HealtcheckTask
from .serializers import NodeSerializer, OfferSerializer
import redis
import json
import aioredis
import requests
from .utils import identify_network
from django.http import JsonResponse, HttpResponse

pool = redis.ConnectionPool(host="redis", port=6379, db=0)
r = redis.Redis(connection_pool=pool)


def globe_data(request):
    # open json file and return data
    with open("/globe_data.geojson") as json_file:
        data = json.load(json_file)
    return JsonResponse(data, safe=False, json_dumps_params={"indent": 4})


async def golem_main_website_index(request):
    if request.method == "GET":
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
        return JsonResponse(
            {"blogs": blogs, "stats": stats, "providers": cheapest_providers},
            safe=False,
            json_dumps_params={"indent": 4},
        )
    else:
        return HttpResponse(status=400)


async def node_wallet(request, wallet):
    """
    Returns all the nodes with the specified wallet address.
    """
    if request.method == "GET":
        data = Node.objects.filter(wallet=wallet)
        if data != None:
            serializer = NodeSerializer(data, many=True)
            return JsonResponse(
                serializer.data, safe=False, json_dumps_params={"indent": 4}
            )
        else:
            return HttpResponse(status=404)

    else:
        return HttpResponse(status=400)


def node(request, yagna_id):
    if request.method == "GET":
        if yagna_id.startswith("0x"):
            data = Node.objects.filter(node_id=yagna_id)
            if data:
                serializer = NodeSerializer(data, many=True)
                return JsonResponse(
                    serializer.data, safe=False, json_dumps_params={"indent": 4}
                )
            else:
                return HttpResponse(status=404)
        else:
            return HttpResponse(status=404)
    else:
        return HttpResponse(status=400)


async def network_online(request):
    if request.method == "GET":
        pool = aioredis.ConnectionPool.from_url(
            "redis://redis:6379/0", decode_responses=True
        )
        r = aioredis.Redis(connection_pool=pool)
        content = await r.get("v2_online")
        data = json.loads(content)
        pool.disconnect()
        return JsonResponse(data, safe=False, json_dumps_params={"indent": 4})
    else:
        return HttpResponse(status=400)


async def network_online_flatmap(request):
    if request.method == "GET":
        pool = aioredis.ConnectionPool.from_url(
            "redis://redis:6379/0", decode_responses=True
        )
        r = aioredis.Redis(connection_pool=pool)
        content = await r.get("v2_online_flatmap")
        data = json.loads(content)
        pool.disconnect()
        return JsonResponse(data, safe=False, json_dumps_params={"indent": 4})
    else:
        return HttpResponse(status=400)


def cheapest_by_cores(request):
    """Displays an array of cheapest offers by number of cores that are NOT computing a task right now"""
    cores = {}
    for i in range(256):
        cores[f"cores_{i}"] = []
    req = requests.get(
        "https://api.coingecko.com/api/v3/coins/ethereum/contract/0x7DD9c5Cba05E151C895FDe1CF355C9A1D5DA6429"
    )
    data = req.json()
    price = data["market_data"]["current_price"]["usd"]
    obj = Offer.objects.filter(
        provider__online=True, runtime="vm", provider__computing_now=False
    ).order_by("monthly_price_glm")
    serializer = OfferSerializer(obj, many=True)
    mainnet_providers = []
    for index, provider in enumerate(serializer.data):
        print(provider["properties"])
        if (
            "golem.com.payment.platform.erc20-mainnet-glm.address"
            in provider["properties"]
        ):
            print("TRUEEEE")
            mainnet_providers.append(provider)
    sorted_pricing_and_specs = sorted(
        mainnet_providers,
        key=lambda element: (
            float(element["properties"]["golem.inf.cpu.threads"]),
            float(element["monthly_price_glm"]),
        ),
    )
    for obj in sorted_pricing_and_specs:
        provider = {}
        provider["name"] = obj["properties"]["golem.node.id.name"]
        provider["id"] = obj["properties"]["id"]
        provider["usd_monthly"] = float(price) * float(obj["monthly_price_glm"])
        provider["cores"] = float(obj["properties"]["golem.inf.cpu.threads"])
        provider["memory"] = float(obj["properties"]["golem.inf.mem.gib"])
        provider["disk"] = float(obj["properties"]["golem.inf.storage.gib"])
        provider["glm"] = float(obj["monthly_price_glm"])
        cores_int = int(obj["properties"]["golem.inf.cpu.threads"])
        cores[f"cores_{cores_int}"].append(provider)

    for i in range(256):
        cores[f"cores_{i}"] = sorted(
            cores[f"cores_{i}"], key=lambda element: element["usd_monthly"]
        )
    return JsonResponse(cores, safe=False, json_dumps_params={"indent": 4})


async def cheapest_offer(request):
    if request.method == "GET":
        pool = aioredis.ConnectionPool.from_url(
            "redis://redis:6379/0", decode_responses=True
        )
        r = aioredis.Redis(connection_pool=pool)
        content = await r.get("v2_cheapest_offer")
        data = json.loads(content)
        pool.disconnect()
        return JsonResponse(data, safe=False, json_dumps_params={"indent": 4})
    else:
        return HttpResponse(status=400)


from rest_framework import status
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from web3 import Web3
from .tasks import healthcheck_provider


@api_view(["POST"])
@permission_classes([IsAuthenticated])
def verify_provider_is_working(request):
    node_id = request.data.get("node_id")
    try:
        provider = Node.objects.get(node_id=node_id)
    except Node.DoesNotExist:
        return Response(
            {"error": "Provider not found."}, status=status.HTTP_404_NOT_FOUND
        )
    if node_id is None:
        return Response(
            {"error": "node_id is required"}, status=status.HTTP_400_BAD_REQUEST
        )

    checksum_address_user = Web3.to_checksum_address(request.user.wallet_address)
    checksum_address_provider = Web3.to_checksum_address(provider.wallet)
    if checksum_address_user != checksum_address_provider:
        return Response(
            {"error": "Provider does not belong to this user."},
            status=status.HTTP_403_FORBIDDEN,
        )
    else:
        find_network = identify_network(provider)
        if find_network == "mainnet":
            network = "polygon"
        else:
            network = "goerli"
        obj = HealtcheckTask.objects.create(
            provider=provider, user=request.user, status="The Healthcheck has been scheduled to queue. We will start in a moment."
        )

        healthcheck_provider.delay(node_id, network, obj.id)
        return Response(
            {"taskId": obj.id, "status": "success"},
            status=status.HTTP_200_OK,
        )


@api_view(["POST"])
def healthcheck_status(request):
    task_status = request.data.get("status")
    task_id = request.data.get("taskId")
    try:
        obj = HealtcheckTask.objects.get(id=task_id)
        obj.status = task_status
        obj.save()
        return Response({"status": "ok"}, status=status.HTTP_200_OK)

    except HealtcheckTask.DoesNotExist:
        return Response(
            {"error": "Healthcheck task not found."}, status=status.HTTP_404_NOT_FOUND
        )


@api_view(["POST"])
def get_healthcheck_status(request):
    task_id = request.data.get("taskId")
    try:
        obj = HealtcheckTask.objects.get(id=task_id)
        return Response({"status": obj.status}, status=status.HTTP_200_OK)

    except HealtcheckTask.DoesNotExist:
        return Response(
            {"error": "Healthcheck task not found."}, status=status.HTTP_404_NOT_FOUND
        )
