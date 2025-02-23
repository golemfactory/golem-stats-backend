from collector.models import Feedback
from datetime import timedelta
from django.utils.timezone import now
from django.db.models import Sum
from api2.models import GolemTransactions
from api2.models import RelayNodes
import requests
import urllib.parse
from rest_framework import status
from rest_framework.decorators import api_view
from rest_framework.response import Response
from .utils import get_stats_data, get_yastats_data
import os
import statistics
import time
from collector.models import (
    Node,
    NetworkStatsMax,
    NetworkStats,
    ProvidersComputing,
    Benchmark,
    Requestors,
)
from .models import APICounter
from .serializers import (
    NodeSerializer,
    NetworkStatsMaxSerializer,
    ProvidersComputingMaxSerializer,
)
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


pool = redis.ConnectionPool(host="redis", port=6379, db=0)
r = redis.Redis(connection_pool=pool)


async def total_api_calls(request):
    if request.method == "GET":
        pool = aioredis.ConnectionPool.from_url(
            "redis://redis:6379/0", decode_responses=True
        )
        r = aioredis.Redis(connection_pool=pool)
        content = await r.get("api_requests")
        data = json.loads(content)
        pool.disconnect()
        return JsonResponse(data, safe=False, json_dumps_params={"indent": 4})
    else:
        return HttpResponse(status=400)


async def median_prices(request):
    if request.method == "GET":
        pool = aioredis.ConnectionPool.from_url(
            "redis://redis:6379/0", decode_responses=True
        )
        r = aioredis.Redis(connection_pool=pool)
        content = await r.get("network_median_pricing")
        data = json.loads(content)
        pool.disconnect()
        return JsonResponse(data, safe=False, json_dumps_params={"indent": 4})
    else:
        return HttpResponse(status=400)


async def average_pricing(request):
    if request.method == "GET":
        pool = aioredis.ConnectionPool.from_url(
            "redis://redis:6379/0", decode_responses=True
        )
        r = aioredis.Redis(connection_pool=pool)
        content = await r.get("network_average_pricing")
        data = json.loads(content)
        pool.disconnect()
        return JsonResponse(data, safe=False, json_dumps_params={"indent": 4})
    else:
        return HttpResponse(status=400)


async def statsmax(request):
    """
    Retrieves network stats over time. (Providers, Cores, Memory, Disk)
    """
    if request.method == "GET":
        pool = aioredis.ConnectionPool.from_url(
            "redis://redis:6379/0", decode_responses=True
        )
        r = aioredis.Redis(connection_pool=pool)
        content = await r.get(
            "stats_max",
        )
        data = json.loads(content)
        pool.disconnect()
        return JsonResponse(data, safe=False, json_dumps_params={"indent": 4})
    else:
        return HttpResponse(status=400)


async def providercomputingmax(request):
    """
    Retrieves providers computing over time. (Highest amount observed during the day)
    """
    if request.method == "GET":
        pool = aioredis.ConnectionPool.from_url(
            "redis://redis:6379/0", decode_responses=True
        )
        r = aioredis.Redis(connection_pool=pool)
        content = await r.get("providers_computing_max")
        data = json.loads(content)
        pool.disconnect()
        return JsonResponse(data, safe=False, json_dumps_params={"indent": 4})
    else:
        return HttpResponse(status=400)


async def avgpricingmax(request):
    """
    Retrieves Average pricing over time. (Start, CPU/h, Per/h)
    """
    if request.method == "GET":
        pool = aioredis.ConnectionPool.from_url(
            "redis://redis:6379/0", decode_responses=True
        )
        r = aioredis.Redis(connection_pool=pool)
        content = await r.get("pricing_average_max")
        data = json.loads(content)
        pool.disconnect()
        return JsonResponse(data, safe=False, json_dumps_params={"indent": 4})
    else:
        return HttpResponse(status=400)


async def medianpricingmax(request):
    """
    Retrieves Average pricing over time. (Start, CPU/h, Per/h)
    """
    if request.method == "GET":
        pool = aioredis.ConnectionPool.from_url(
            "redis://redis:6379/0", decode_responses=True
        )
        r = aioredis.Redis(connection_pool=pool)
        content = await r.get("pricing_median_max")
        data = json.loads(content)
        pool.disconnect()
        return JsonResponse(data, safe=False, json_dumps_params={"indent": 4})
    else:
        return HttpResponse(status=400)


async def online_nodes(request):
    """
    List all online nodes.
    """
    if request.method == "GET":

        return JsonResponse(
            {
                "deprecation": "This endpoint has been sunset due to instability. Please use v2/network/online instead."
            },
            safe=False,
            json_dumps_params={"indent": 4},
        )
    else:
        return HttpResponse(status=400)


async def activity_graph_provider(request, yagna_id):
    end = round(time.time())
    start = end - 86400  # 24 hours

    query = (
        f'activity_provider_created{{exported_instance="{yagna_id}", exported_job=~"community.1"}}'
        " - "
        f'activity_provider_destroyed{{exported_instance="{yagna_id}", exported_job=~"community.1"}}'
    )

    encoded_query = urllib.parse.quote(query)

    domain = (
        os.environ.get("STATS_URL")
        + "api/datasources/uid/dec5owmc8gt8ge/resources/api/v1/query_range?"
        + f"query={encoded_query}&start={start}&end={end}&step=120"
    )
    data = await get_yastats_data(domain)
    if data[1] == 200:
        return JsonResponse(data[0], json_dumps_params={"indent": 4})


async def payments_last_n_hours_provider(request, yagna_id, hours):
    now = round(time.time())
    domain = (
        os.environ.get("STATS_URL")
        + f'api/datasources/uid/dec5owmc8gt8ge/resources/api/v1/query?query=sum(increase(payment_amount_received%7Binstance%3D~"{yagna_id}"%2C%20job%3D~"community.1"%7D%5B{hours}h%5D)%2F10%5E9)&time={now}'
    )
    data = await get_yastats_data(domain)
    if data[1] == 200:
        if data[0]["data"]["result"]:
            content = {"earnings": data[0]["data"]["result"][0]["value"][1]}
            return JsonResponse(content, json_dumps_params={"indent": 4})
        else:
            content = {"earnings": []}
            return JsonResponse(content, json_dumps_params={"indent": 4})
    else:
        content = {"earnings": []}
        return JsonResponse(content, json_dumps_params={"indent": 4})


async def yagna_releases(request):
    if request.method == "GET":
        pool = aioredis.ConnectionPool.from_url(
            "redis://redis:6379/0", decode_responses=True
        )
        r = aioredis.Redis(connection_pool=pool)
        content = await r.get("yagna_releases")
        data = json.loads(content)
        pool.disconnect()
        return JsonResponse(data, safe=False, json_dumps_params={"indent": 4})
    else:
        return HttpResponse(status=400)


async def payments_earnings_provider(request, yagna_id):
    now = round(time.time())
    time_intervals = ["24", "168", "720", "2160"]

    earnings = {}
    base_url = os.environ.get(
        "STATS_URL") + "api/datasources/uid/dec5owmc8gt8ge/resources/api/v1/query"

    for interval in time_intervals:
        query_url = f'{base_url}?query=sum(increase(payment_amount_received%7Binstance%3D~"{yagna_id}"%2C%20job%3D~"community.1"%7D%5B{interval}h%5D)%2F10%5E9)&time={now}'
        data = await get_yastats_data(query_url)

        if data[1] == 200 and data[0]["data"]["result"]:
            earnings[interval] = data[0]["data"]["result"][0]["value"][1]
        else:
            earnings[interval] = 0.0  # If no data is found, set earnings to 0

    return JsonResponse(earnings, json_dumps_params={"indent": 4})


def payments_earnings_provider_new(request, yagna_id):
    now = int(time.time())
    hour_intervals = [24, 168, 720, 2160]
    base_url = "http://erc20-api/erc20/api/stats/transfers?chain=137&receiver="

    earnings = {}

    for interval in hour_intervals:
        epoch = now - (interval * 3600)
        url = f"{base_url}{yagna_id}&from={epoch}&to={now}"
        response = requests.get(url)

        if response.status_code == 200:
            data = response.json()
            transfers = data.get("transfers", [])

            from_addrs = {t["fromAddr"] for t in transfers}
            matched_addrs = set(
                Requestors.objects.filter(node_id__in=from_addrs).values_list(
                    "node_id", flat=True
                )
            ).union(
                set(
                    RelayNodes.objects.filter(node_id__in=from_addrs).values_list(
                        "node_id", flat=True
                    )
                )
            )

            total_amount_wei_matched = 0
            for t in transfers:
                if t["fromAddr"] in matched_addrs:
                    total_amount_wei_matched += int(t["tokenAmount"])

            earnings[str(interval)] = total_amount_wei_matched / 1e18
        else:
            earnings[str(interval)] = 0.0
        total_earnings = (
            GolemTransactions.objects.filter(
                receiver=yagna_id, tx_from_golem=True
            ).aggregate(Sum("amount"))["amount__sum"]
            or 0.0
        )
        earnings["total"] = total_earnings

    return JsonResponse(earnings, json_dumps_params={"indent": 4})


async def total_tasks_computed(request, yagna_id):
    now = round(time.time())
    domain = (
        os.environ.get("STATS_URL")
        + f'api/datasources/uid/dec5owmc8gt8ge/resources/api/v1/query?query=sum(increase(market_agreements_provider_terminated_reason%7Binstance%3D~"{yagna_id}"%2C%20reason%3D"Success"%7D%5B90d%5D))&time={now}'
    )
    data = await get_yastats_data(domain)
    if data[1] == 200:
        if data[0]["data"]["result"]:
            output = int(float(data[0]["data"]["result"][0]["value"][1]))
            content = {"tasks_computed_total": output}
            return JsonResponse(content, json_dumps_params={"indent": 4})
        else:
            content = {"tasks_computed_total": []}
            return JsonResponse(content, json_dumps_params={"indent": 4})
    else:
        content = {"tasks_computed_total": []}
        return JsonResponse(content, json_dumps_params={"indent": 4})


async def provider_seconds_computed_total(request, yagna_id):
    now = round(time.time())
    domain = (
        os.environ.get("STATS_URL")
        + f'api/datasources/uid/dec5owmc8gt8ge/resources/api/v1/query?query=sum(increase(activity_provider_usage_1%7Binstance%3D~"{yagna_id}"%7D%5B90d%5D))&time={now}'
    )
    data = await get_yastats_data(domain)
    if data[1] == 200:
        if data[0]["data"]["result"]:
            output = data[0]["data"]["result"][0]["value"][1]
            content = {"seconds_computed": output}
            return JsonResponse(content, json_dumps_params={"indent": 4})
        else:
            content = {"seconds_computed": []}
            return JsonResponse(content, json_dumps_params={"indent": 4})
    else:
        content = {"seconds_computed": []}
        return JsonResponse(content, json_dumps_params={"indent": 4})


async def provider_computing(request, yagna_id):
    now = round(time.time())
    domain = (
        os.environ.get("STATS_URL")
        + f'api/datasources/uid/dec5owmc8gt8ge/resources/api/v1/query?query=activity_provider_created%7Binstance%3D~"{yagna_id}"%2C%20job%3D~"community.1"%7D%20-%20activity_provider_destroyed%7Binstance%3D~"{yagna_id}"%2C%20job%3D~"community.1"%7D&time={now}'
    )
    data = await get_yastats_data(domain)
    if data[1] == 200:
        if data[0]["data"]["result"]:
            content = {"computing": data[0]["data"]["result"][0]["value"][1]}
            return JsonResponse(content, json_dumps_params={"indent": 4})
        else:
            content = {"computing": []}
            return JsonResponse(content, json_dumps_params={"indent": 4})
    else:
        content = {"computing": []}
        return JsonResponse(content, json_dumps_params={"indent": 4})


def node(request, yagna_id):
    """
    Retrieves data about a specific node.
    """
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


def hybrid_net_winner_indexer(request, wallet):
    """
    Retrieves all nodes that has been connected to the wallet address. This is used in the hybrid network testing to find the winner.
    """
    if request.method == "GET":
        data = Node.objects.filter(wallet=wallet)
        whitelist = [
            "0x043fda746bd025048d9e14b15d14abd81a08c742",
            "0x0d7b81ffcfe94cca04839dc24962b3ce78982d63",
            "0x17e55f72f14253ea7bc13507551b0d24112b56a7",
            "0x185a354fcd18f5c5279185a826ea623e1c392fcb",
            "0x1c36dcb6e0bae0236cbc138e3272b3531473958d",
            "0x2fd82fd6339c9bf77c12ea1519a3abdf84eb1b87",
            "0x49f6e43cf45329b1db3069c7eb9586220e1225ea",
            "0x69e86e07db784f48f050592f2ab674b366be2e65",
            "0x740645a66df793c49ae98ebcf45d9585c8058104",
            "0x747de95483bd5913709f486b7c945574839be0d9",
            "0x7a7533209b6ec610b33632b1ba417e715d2f9a18",
            "0x8dd978d6876fd3b7e1dfdb4da5983d98e77b0857",
            "0x929e27c1a41f5be20fae73c3dfe5d5a8a247ca73",
            "0x95aabcecd28dc0e8351d2e22b63019f82d78f66c",
            "0xe1904e058637117dd6a152f5f93dc4c8e1ba4e02",
            "0xe6c8c933d5649b2ceeade80e0d47f0c2270c8dbf",
            "0xeb7270a90c11234867a1de661698638670789cfa",
            "0xf240569d3d6c026a935d40e4a93e36f03a6fcc01",
            "0x9da7517232f489e4e15807bb778f6579ae09f7e9",
            "0xbbf80d77766a13a9504191d846d18b33901495df",
            "0x6d7e6e5a8917e7cdc821c2fc3bc10bc930aa8832",
            "0x4c3c9df6909b69f3f0b45f2debe3b8dd2a833c7a",
            "0xaecffe3496a945846998f46511e51bf8c5c3f0b9",
            "0x5c5f3fefe262455095d53861ba31ae7c0cb455c3",
            "0x61ce552171bc2d420f7a210794fa76f15aa65687",
            "0xb56a7bf63d6bab1e54fe9aca28cadc0e28c5cdf2",
            "0x7726eae6058e68a0f6ba0e6224bb25e5d3e12ba1",
            "0xf6d4a6c4b4e8664f9aa7f0e6db299fa86cf44f53",
            "0x32e7d0abd790884826884c76f2c10604e9d76306",
            "0x3bd5fd01af0dd17c540a5fc3133c595d4ce0f54b",
            "0x0680c57bf86c47c9d7f164dd158970675b999d82",
            "0xe77f62b69089a2a7cc1f1550f06320091fd4b854",
            "0x07fee8568f692f859638c6771dafb8c2e5c5f06f",
            "0x4ca6e20cd5d3eff0395492e77de26eb85d9f0106",
            "0x50edface6547b08c68b977c0fcaac2b3412e807d",
            "0x675b17619a9a1fdea7f3da2c85b28c80dfdb356f",
            "0x8809a144e43cac82b744a36bdfc2b9d09f2d2507",
            "0x960deebd6911ef97d4656cedb147b091422ad9e7",
            "0x9ccf3d2231981387d23b0eb29cc5c9bd09fd01a7",
            "0x177fc2c7950d6fd3ade6f18e23f70c35b6e9afa4",
            "0x330fad201c2212f04e31c560150bc31feca40344",
            "0x3d341a37a661842bef15a4907e32db9450dee532",
            "0x416cf10a7d0433461bfbdbf15e270c2c40804a69",
            "0x731bce76a8142ef13433bffc72e3ee8d89f86565",
            "0x7cac41c8ff3b6e3aded122a0a2c7bcced301be60",
            "0x852156c4b5baa6c0c4cb210dd7570b5761e01c4c",
            "0xaa3cd4e5f2d01690ac20011ce30b9dbe3f39893a",
            "0xeac7d10f780cc38304069b7021564107f9480c76",
            "0xec433562dff383ab9fd3a8ec99e4fb86b24c087d",
            "0x30f08c0663d437adcb30f48a378ace6ff72851e0",
            "0x421c1837aeba0f5493c938146021efe889980c32",
            "0x8b54c09f47ca8ceedeab3fb2cfc0bc0c63fa00ff",
            "0x1da1a60f1f85c6d0b1330165561da55dab45c5f9",
            "0x32913dcbef8ab3a1753e90cfaabb53860d079230",
            "0x69d564e610df5a33b984f8a922a4a629281c834f",
            "0x749536bd3a193d223316181dce4d8602a8873f85",
            "0x8884798047ff6991088b07081c9570d9f39797d1",
            "0x8c0375fee584b9593f31ee8c145d88d974fbeba7",
            "0x8d882330b6345757d82d5034a3d154c3b7f134b7",
            "0x8e96103a97e1d2095b01c69449326f686bbdf117",
            "0xa04a6fae203d6054f169bf11fb2d0f01abaa07ef",
            "0xadaf51cff235c6508724a809aa4f3c98bb8c382a",
            "0xf8b467771a7bcb338046ddbed7515b89ab24d343",
            "0x03b3d92b3a742e2b66437d21001b6c7fc22b8b2a",
            "0x503717e376910af23de6e8e565f5405a9d86cc49",
            "0x7a96642c50476eb7f9f096356455f1c2a1036398",
            "0x835a0c0a134c639d9d8f39c1b65d741025adf59b",
            "0xb588a6ede7daf5fa2e0d025e169868f198ac0824",
            "0xec6027c5adcc675d92a5551b6cd57cff38d066c4",
            "0x3e806bb16c74d401ea628d4e9d2755ec348de0eb",
            "0xd78ad96553e33a14dab722c175d3885d66f0b601",
            "0x45b728612826823971e6075bc6750d6e37fec067",
            "0x4d8b65a56e50da27961f0231d20a6c07f9465cc2",
            "0x24d9ff8d702a1740129eecbaab3016d26ff7c5fb",
            "0x0acca485e5f290528748f256c9f46d0833392782",
            "0xc0d82106bb4bea9ea8ffdf59ed59efb34f30fed7",
            "0x139f4c7fd736dc88198a17f9f9b4d083027eda2b",
            "0x1c0d2adffa9b06679f9fb04f13de397e3f90c094",
            "0x77cb1deed3c840069c5e8ce3d643ff8d2cfef7cf",
            "0x15324a71d71ab7c37552c012101882352290d801",
            "0x462735c609e564d44287c45757d4a572df6de05c",
            "0x21d2afd6c7e73d2d211a583be04eebf44f37a59c",
            "0xbd61877181ce199dd570a9c421008f13929f5bbd",
            "0xphillip",
        ]
        if data:
            eligible_addresses = []
            for node in data:
                if node.node_id in whitelist:
                    eligible_addresses.append(node.node_id)
            return JsonResponse(
                eligible_addresses, safe=False, json_dumps_params={"indent": 4}
            )
        else:
            return HttpResponse(status=404)
    else:
        return HttpResponse(status=400)


def latest_nodes(request):
    """
    Lists all index nodes over time and orders it by the latest node discovered.
    """
    if request.method == "GET":
        data = Node.objects.all().order_by("-created_at")
        serializer = NodeSerializer(data, many=True)
        return JsonResponse(
            serializer.data, safe=False, json_dumps_params={"indent": 4}
        )
    else:
        return HttpResponse(status=400)


def latest_nodes_by_number(request, number):
    """
    Lists n amount of the latest nodes indexed.
    """
    if request.method == "GET":
        data = Node.objects.all().order_by("-created_at")[:number]
        serializer = NodeSerializer(data, many=True)
        return JsonResponse(
            serializer.data, safe=False, json_dumps_params={"indent": 4}
        )
    else:
        return HttpResponse(status=400)


async def show_endpoint_count(request):
    """
    Lists the amount of times an endpoint has been requested.
    """
    if request.method == "GET":
        endpoint = request.GET["endpoint"]
        data = APICounter.objects.filter(endpoint=endpoint).count()
        return JsonResponse(data, safe=False, json_dumps_params={"indent": 4})
    else:
        return HttpResponse(status=400)


def computing_total(request):
    """
    Retrieves data about a specific node.
    """
    if request.method == "GET":
        data = ProvidersComputing.objects.all().order_by("-total")
        serializer = ProvidersComputingMaxSerializer(data, many=True)
        return JsonResponse(
            serializer.data, safe=False, json_dumps_params={"indent": 4}
        )
    else:
        return HttpResponse(status=400)


async def stats_30m(request):
    """
    Network stats past 30 minutes.
    """
    if request.method == "GET":
        pool = aioredis.ConnectionPool.from_url(
            "redis://redis:6379/0", decode_responses=True
        )
        r = aioredis.Redis(connection_pool=pool)
        content = await r.get("stats_30m")
        data = json.loads(content)
        pool.disconnect()
        return JsonResponse(data, safe=False, json_dumps_params={"indent": 4})
    else:
        return HttpResponse(status=400)


def node_wallet(request, wallet):
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


async def general_stats(request):
    """
    List network stats for online nodes.
    """
    if request.method == "GET":
        pool = aioredis.ConnectionPool.from_url(
            "redis://redis:6379/0", decode_responses=True
        )
        r = aioredis.Redis(connection_pool=pool)
        content = await r.get("online_stats")
        data = json.loads(content)
        pool.disconnect()
        return JsonResponse(data, safe=False, json_dumps_params={"indent": 4})
    else:
        return HttpResponse(status=400)


async def network_utilization(request):
    """
    Queries the networks utilization from a start date to the end date specified, and returns
    timestamps in ms along with providers computing.
    """
    if request.method == "GET":
        pool = aioredis.ConnectionPool.from_url(
            "redis://redis:6379/0", decode_responses=True
        )
        r = aioredis.Redis(connection_pool=pool)
        content = await r.get("network_utilization")
        data = json.loads(content)
        pool.disconnect()
        return JsonResponse(data, safe=False, json_dumps_params={"indent": 4})
    else:
        return HttpResponse(status=400)


async def network_versions(request):
    """
    Queries the networks nodes for their yagna versions
    """
    if request.method == "GET":
        pool = aioredis.ConnectionPool.from_url(
            "redis://redis:6379/0", decode_responses=True
        )
        r = aioredis.Redis(connection_pool=pool)
        content = await r.get("network_versions")
        data = json.loads(content)
        pool.disconnect()
        return JsonResponse(data, safe=False, json_dumps_params={"indent": 4})
    else:
        return HttpResponse(status=400)


async def providers_computing_currently(request):
    """
    Returns how many providers are currently computing a task.
    """
    if request.method == "GET":
        pool = aioredis.ConnectionPool.from_url(
            "redis://redis:6379/0", decode_responses=True
        )
        r = aioredis.Redis(connection_pool=pool)
        content = await r.get("computing_now")
        data = json.loads(content)
        pool.disconnect()
        return JsonResponse(data, safe=False, json_dumps_params={"indent": 4})
    else:
        return HttpResponse(status=400)


async def providers_average_earnings(request):
    """
    Returns providers average earnings per task in the last hour.
    """
    if request.method == "GET":
        pool = aioredis.ConnectionPool.from_url(
            "redis://redis:6379/0", decode_responses=True
        )
        r = aioredis.Redis(connection_pool=pool)
        content = await r.get("provider_average_earnings")
        data = json.loads(content)
        pool.disconnect()
        return JsonResponse(data, safe=False, json_dumps_params={"indent": 4})
    else:
        return HttpResponse(status=400)


async def network_earnings_24h(request):
    """
    Returns the earnings for the whole network the last n hours.
    """
    if request.method == "GET":
        pool = aioredis.ConnectionPool.from_url(
            "redis://redis:6379/0", decode_responses=True
        )
        r = aioredis.Redis(connection_pool=pool)
        content = await r.get("network_earnings_24h")
        data = json.loads(content)
        pool.disconnect()
        return JsonResponse(data, safe=False, json_dumps_params={"indent": 4})
    else:
        return HttpResponse(status=400)


async def network_total_earnings(request):
    """
    Returns the earnings for the whole network over time.
    """
    if request.method == "GET":
        pool = aioredis.ConnectionPool.from_url(
            "redis://redis:6379/0", decode_responses=True
        )
        r = aioredis.Redis(connection_pool=pool)
        content = await r.get("network_total_earnings")
        data = json.loads(content)
        pool.disconnect()
        return JsonResponse(data, safe=False, json_dumps_params={"indent": 4})
    else:
        return HttpResponse(status=400)


async def network_earnings_6h(request):
    """
    Returns the earnings for the whole network the last n hours.
    """
    if request.method == "GET":
        pool = aioredis.ConnectionPool.from_url(
            "redis://redis:6379/0", decode_responses=True
        )
        r = aioredis.Redis(connection_pool=pool)
        content = await r.get("network_earnings_6h")
        data = json.loads(content)
        pool.disconnect()
        return JsonResponse(data, safe=False, json_dumps_params={"indent": 4})
    else:
        return HttpResponse(status=400)


async def network_earnings_overview(request):
    """
    Returns the earnings for the whole network over time for various time frames,
    including the total network earnings.
    """
    if request.method == "GET":
        time_frames = [6, 24, 168, 720, 2160]  # Time frames in hours
        pool = aioredis.ConnectionPool.from_url(
            "redis://redis:6379/0", decode_responses=True
        )
        r = aioredis.Redis(connection_pool=pool)

        all_data = {}
        for hours in time_frames:
            key = f"network_earnings_{hours}h"
            content = await r.get(key)
            if content:
                data = json.loads(content)
                all_data[key] = data
            else:
                all_data[key] = None  # Or handle the missing data as needed

        # Fetching total network earnings
        total_earnings_content = await r.get("network_total_earnings")
        if total_earnings_content:
            total_earnings_data = json.loads(total_earnings_content)
            all_data["network_total_earnings"] = total_earnings_data
        else:
            all_data["network_total_earnings"] = None  # Or handle as needed

        pool.disconnect()
        return JsonResponse(all_data, safe=False, json_dumps_params={"indent": 4})
    else:
        return HttpResponse(status=400)


async def network_earnings_overview_new(request):
    if request.method == "GET":
        pool = aioredis.ConnectionPool.from_url(
            "redis://redis:6379/0", decode_responses=True
        )
        r = aioredis.Redis(connection_pool=pool)
        content = await r.get("network_earnings_overview_new")
        data = json.loads(content)
        pool.disconnect()
        return JsonResponse(data, safe=False, json_dumps_params={"indent": 4})
    else:
        return HttpResponse(status=400)


async def requestors(request):
    """
    Returns all the requestors seen on the network and the tasks requested amount.
    """
    if request.method == "GET":
        pool = aioredis.ConnectionPool.from_url(
            "redis://redis:6379/0", decode_responses=True
        )
        r = aioredis.Redis(connection_pool=pool)
        content = await r.get("requestors")
        data = json.loads(content)
        pool.disconnect()
        return JsonResponse(data, safe=False, json_dumps_params={"indent": 4})
    else:
        return HttpResponse(status=400)


async def market_agreement_termination_reason(request):
    """
    Returns the reasons for market agreements termination.
    """
    if request.method == "GET":
        pool = aioredis.ConnectionPool.from_url(
            "redis://redis:6379/0", decode_responses=True
        )
        r = aioredis.Redis(connection_pool=pool)
        content = await r.get("market_agreement_termination_reasons")
        data = json.loads(content)
        pool.disconnect()
        return JsonResponse(data, safe=False, json_dumps_params={"indent": 4})
    else:
        return HttpResponse(status=400)


async def paid_invoices_1h(request):
    """
    Returns the percentage of invoices paid during the last hour.
    """
    if request.method == "GET":
        pool = aioredis.ConnectionPool.from_url(
            "redis://redis:6379/0", decode_responses=True
        )
        r = aioredis.Redis(connection_pool=pool)
        content = await r.get("paid_invoices_1h")
        data = json.loads(content)
        pool.disconnect()
        return JsonResponse(data, safe=False, json_dumps_params={"indent": 4})
    else:
        return HttpResponse(status=400)


async def provider_invoice_accepted_percentage(request):
    """
    Returns the percentage of invoices accepted by the provider that they have issued to the requestor.
    """
    if request.method == "GET":
        pool = aioredis.ConnectionPool.from_url(
            "redis://redis:6379/0", decode_responses=True
        )
        r = aioredis.Redis(connection_pool=pool)
        content = await r.get("provider_accepted_invoice_percentage")
        data = json.loads(content)
        pool.disconnect()
        return JsonResponse(data, safe=False, json_dumps_params={"indent": 4})
    else:
        return HttpResponse(status=400)


def store_benchmarks(request):
    """
    Store benchmark results
    """
    if request.method == "POST":
        received_json_data = json.loads(request.body)
        if request.META["HTTP_STATSTOKEN"] == os.getenv("STATS_TOKEN"):
            for obj in received_json_data:
                data = Node.objects.get(node_id=obj["provider_id"])
                benchmark = Benchmark.objects.create(
                    benchmark_score=obj["score"],
                    provider=data,
                    type=request.META["HTTP_BENCHMARKTYPE"],
                )
            return HttpResponse(status=200)
        else:
            return HttpResponse(status=400)
    else:
        return HttpResponse(status=400)


def store_feedback(request):
    """
    Store feedback results
    """
    if request.method == "POST":
        received_json_data = json.loads(request.body)
        store_feedback = Feedback.objects.create(
            feedback=received_json_data["feedback"],
        )
        return HttpResponse(status=200)
    else:
        return HttpResponse(status=400)
