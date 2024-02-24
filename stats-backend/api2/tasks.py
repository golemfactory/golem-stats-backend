from core.celery import app
from celery import Celery
import json
import subprocess
import os
from .models import Node, Offer, GLM, EC2Instance
from django.utils import timezone
import tempfile
import redis
from .serializers import NodeSerializer, OfferSerializer
import calendar
import datetime
import requests
from api.serializers import FlatNodeSerializer
from collector.models import Node as NodeV1
from django.db.models import F
from django.db.models.functions import Abs
from decimal import Decimal
from .utils import (
    get_pricing,
    get_ec2_products,
    find_cheapest_price,
    has_vcpu_memory,
    round_to_three_decimals,
)

pool = redis.ConnectionPool(host="redis", port=6379, db=0)
r = redis.Redis(connection_pool=pool)

from django.db.models.functions import TruncHour, TruncDay
from collections import defaultdict
import json
from collector.models import NetworkStats
from django.db.models import Avg
from datetime import datetime, timedelta


@app.task
def network_historical_stats_to_redis_v2():
    now = datetime.now()
    four_weeks_ago = now - timedelta(weeks=4)

    def data_with_granularity(start_date, end_date, granularity):
        return (
            NetworkStats.objects.filter(date__range=(start_date, end_date))
            .annotate(timestamp=granularity("date"))
            .values("timestamp")
            .annotate(
                online=Avg("online"),
                cores=Avg("cores"),
                memory=Avg("memory"),
                disk=Avg("disk"),
            )
            .order_by("timestamp")
        )

    daily_data_past_4_weeks = data_with_granularity(four_weeks_ago, now, TruncDay)
    hourly_data_past_week = data_with_granularity(
        now - timedelta(weeks=1), now, TruncHour
    )

    formatted_data = {"1w": [], "2w": [], "4w": [], "All": []}

    def append_data(data_source, key):
        for entry in data_source:
            formatted_entry = {
                "date": entry["timestamp"].timestamp(),
                "online": round(entry["online"]),
                "cores": round(entry["cores"]),
                "memory": round(entry["memory"] / 1024, 2),
                "disk": round(entry["disk"] / 1024, 2),
            }
            formatted_data[key].append(formatted_entry)

    append_data(hourly_data_past_week, "1w")
    append_data(daily_data_past_4_weeks, "4w")

    filtered_2w_data = [
        d
        for d in daily_data_past_4_weeks
        if d["timestamp"] >= (now - timedelta(weeks=2))
    ]
    append_data(filtered_2w_data, "2w")

    all_time_data = data_with_granularity(
        NetworkStats.objects.earliest("date").date, now, TruncDay
    )
    append_data(all_time_data, "All")

    r.set("network_historical_stats_v2", json.dumps(formatted_data))


@app.task
def v2_network_online_to_redis():
    # Fetch and process data from the external domain
    response = requests.get(
        "https://reputation.dev-test.golem.network/v1/providers/scores"
    )
    if response.status_code == 200:
        external_data = response.json()
        success_rate_mapping = {
            provider["providerId"]: provider["scores"]["successRate"]
            for provider in external_data["providers"]
        }

        # Fetch your existing nodes
        data = Node.objects.filter(online=True)
        serializer = NodeSerializer(data, many=True)
        serialized_data = serializer.data

        # Attach successRate to each node if the providerId matches
        for node in serialized_data:
            node_id = node["node_id"]
            if node_id in success_rate_mapping:
                node["taskReputation"] = success_rate_mapping[node_id]
            else:
                node["taskReputation"] = None

        # Serialize and save to Redis
        test = json.dumps(serialized_data, default=str)
        r.set("v2_online", test)
    else:
        print(
            "Failed to retrieve data from the reputation system!", response.status_code
        )
        pass


@app.task
def v2_network_online_to_redis_flatmap():
    data = NodeV1.objects.filter(online=True)
    serializer = FlatNodeSerializer(data, many=True)
    test = json.dumps(serializer.data)
    r.set("v2_online_flatmap", test)


@app.task
def v2_cheapest_offer():
    recently = timezone.now() - timezone.timedelta(minutes=5)
    data = Offer.objects.filter(
        runtime="vm", updated_at__range=(recently, timezone.now())
    ).order_by("-monthly_price_glm")
    serializer = OfferSerializer(data, many=True)
    sorted_data = json.dumps(serializer.data, default=str)

    r.set("v2_cheapest_offer", sorted_data)


@app.task
def latest_blog_posts():
    req = requests.get(
        f"https://blog.golemproject.net/ghost/api/v3/content/posts/?key={os.environ.get('BLOG_API_KEY')}&include=tags,authors&limit=3"
    )
    data = json.dumps(req.json())
    r.set("v2_index_blog_posts", data)


@app.task
def v2_cheapest_provider():
    req = requests.get(
        "https://api.coingecko.com/api/v3/coins/ethereum/contract/0x7DD9c5Cba05E151C895FDe1CF355C9A1D5DA6429"
    )
    data = req.json()
    price = data["market_data"]["current_price"]["usd"]
    obj = Offer.objects.filter(runtime="vm", provider__online=True).order_by(
        "monthly_price_glm"
    )
    serializer = OfferSerializer(obj, many=True)
    mainnet_providers = []
    for index, provider in enumerate(serializer.data):
        if (
            "golem.com.payment.platform.erc20-mainnet-glm.address"
            in provider["properties"]
        ):
            mainnet_providers.append(provider)
    sorted_pricing_and_specs = sorted(
        mainnet_providers,
        key=lambda element: (
            float(element["properties"]["golem.inf.cpu.threads"]),
            float(element["monthly_price_glm"]),
        ),
    )
    two_cores = [
        {
            "name": "Digital Ocean",
            "img": "/do-logo.svg",
            "usd_monthly": "15",
            "bandwidth": "3",
            "cores": 2,
            "memory": "1",
            "disk": "25",
            "glm": float(price) * 15,
        },
        {
            "name": "Amazon Web Services",
            "img": "/aws-logo.svg",
            "usd_monthly": "15.23",
            "bandwidth": "Unlimited",
            "cores": 2,
            "memory": "1",
            "disk": "25",
            "glm": float(price) * 15.23,
        },
        {
            "name": "Google Cloud Platform",
            "img": "/gcp-logo.svg",
            "usd_monthly": "10.37",
            "bandwidth": "Unlimited",
            "cores": 2,
            "memory": "1",
            "disk": "25",
            "glm": float(price) * 10.37,
        },
        {
            "name": "Azure",
            "img": "/azure-logo.svg",
            "usd_monthly": "15.11",
            "bandwidth": "6",
            "cores": 2,
            "memory": "1",
            "disk": "25",
            "glm": float(price) * 15.11,
        },
    ]
    eight_cores = [
        {
            "name": "Digital Ocean",
            "img": "/do-logo.svg",
            "usd_monthly": "80",
            "bandwidth": "6",
            "cores": 8,
            "memory": "16",
            "disk": "320",
            "glm": float(price) * 80,
        },
        {
            "name": "Amazon Web Services",
            "img": "/aws-logo.svg",
            "usd_monthly": "121.81",
            "bandwidth": "Unlimited",
            "cores": 8,
            "memory": "16",
            "disk": "320",
            "glm": float(price) * 121.81,
        },
        {
            "name": "Google Cloud Platform",
            "img": "/gcp-logo.svg",
            "usd_monthly": "208.47",
            "bandwidth": "Unlimited",
            "cores": 8,
            "memory": "32",
            "disk": "320",
            "glm": float(price) * 208.47,
        },
        {
            "name": "Azure",
            "img": "/azure-logo.svg",
            "usd_monthly": "121.18",
            "cores": 8,
            "memory": "16",
            "bandwidth": "6",
            "disk": "320",
            "glm": float(price) * 121.18,
        },
    ]
    thirtytwo_cores = [
        {
            "name": "Digital Ocean",
            "img": "/do-logo.svg",
            "usd_monthly": "640",
            "bandwidth": "9",
            "cores": 32,
            "memory": "64",
            "disk": "400",
            "glm": float(price) * 640,
        },
        {
            "name": "Amazon Web Services",
            "img": "/aws-logo.svg",
            "usd_monthly": "834.24",
            "bandwidth": "Unlimited",
            "cores": 32,
            "memory": "64",
            "disk": "400",
            "glm": float(price) * 834.24,
        },
        {
            "name": "Google Cloud Platform",
            "img": "/gcp-logo.svg",
            "usd_monthly": "746.04",
            "bandwidth": "Unlimited",
            "cores": 32,
            "memory": "64",
            "disk": "400",
            "glm": float(price) * 746.04,
        },
        {
            "name": "Azure",
            "img": "/azure-logo.svg",
            "usd_monthly": "1310.13",
            "bandwidth": "1",
            "cores": 32,
            "memory": "64",
            "disk": "256",
            "glm": float(price) * 1310.13,
        },
    ]
    sixtyfour_cores = [
        {
            "name": "Digital Ocean",
            "img": "/do-logo.svg",
            "usd_monthly": "1200",
            "bandwidth": "9",
            "cores": 40,
            "memory": "160",
            "disk": "500",
            "glm": float(price) * 1200,
        },
        {
            "name": "Amazon Web Services",
            "img": "/aws-logo.svg",
            "usd_monthly": "1638.48",
            "bandwidth": "Unlimited",
            "cores": 64,
            "memory": "64",
            "disk": "500",
            "glm": float(price) * 1638.48,
        },
        {
            "name": "Google Cloud Platform",
            "img": "/gcp-logo.svg",
            "usd_monthly": "1914.62",
            "bandwidth": "Unlimited",
            "cores": 60,
            "memory": "240",
            "disk": "500",
            "glm": float(price) * 1914.62,
        },
        {
            "name": "Azure",
            "img": "/azure-logo.svg",
            "usd_monthly": "2688.37",
            "bandwidth": "1",
            "cores": 64,
            "memory": "256",
            "disk": "512",
            "glm": float(price) * 2688.37,
        },
    ]
    for obj in sorted_pricing_and_specs:
        provider = {}
        provider["name"] = "Golem Network"
        provider["node_id"] = obj["properties"]["id"]
        provider["img"] = "/golem.png"
        provider["usd_monthly"] = float(price) * float(obj["monthly_price_glm"])
        provider["cores"] = float(obj["properties"]["golem.inf.cpu.threads"])
        provider["memory"] = float(obj["properties"]["golem.inf.mem.gib"])
        provider["bandwidth"] = "Unlimited"
        provider["disk"] = float(obj["properties"]["golem.inf.storage.gib"])
        provider["glm"] = float(obj["monthly_price_glm"])
        if (
            float(obj["properties"]["golem.inf.cpu.threads"]) == 2
            and len(two_cores) == 4
        ):
            two_cores.append(provider)
        elif (
            float(obj["properties"]["golem.inf.cpu.threads"]) >= 2
            and len(two_cores) == 4
        ):
            two_cores.append(provider)
        if (
            float(obj["properties"]["golem.inf.cpu.threads"]) == 8
            and len(eight_cores) == 4
        ):
            eight_cores.append(provider)
        elif (
            float(obj["properties"]["golem.inf.cpu.threads"]) >= 8
            and len(eight_cores) == 4
        ):
            eight_cores.append(provider)
        if (
            float(obj["properties"]["golem.inf.cpu.threads"]) == 32
            and len(thirtytwo_cores) == 4
        ):
            thirtytwo_cores.append(provider)
        elif (
            float(obj["properties"]["golem.inf.cpu.threads"]) >= 32
            and len(thirtytwo_cores) == 4
        ):
            thirtytwo_cores.append(provider)
        if (
            float(obj["properties"]["golem.inf.cpu.threads"]) == 64
            and len(sixtyfour_cores) == 4
        ):
            sixtyfour_cores.append(provider)
        elif (
            float(obj["properties"]["golem.inf.cpu.threads"]) >= 64
            and len(sixtyfour_cores) == 4
        ):
            sixtyfour_cores.append(provider)

    sorted_two = sorted(two_cores, key=lambda element: (float(element["usd_monthly"])))
    sorted_eight = sorted(
        eight_cores, key=lambda element: (float(element["usd_monthly"]))
    )
    sorted_thirtytwo = sorted(
        thirtytwo_cores, key=lambda element: (float(element["usd_monthly"]))
    )
    sorted_sixtyfour = sorted(
        sixtyfour_cores, key=lambda element: (float(element["usd_monthly"]))
    )
    data = json.dumps(
        {
            "2": sorted_two,
            "8": sorted_eight,
            "32": sorted_thirtytwo,
            "64": sorted_sixtyfour,
        }
    )
    r.set("v2_cheapest_provider", data)


@app.task
def get_current_glm_price():
    url = "https://api.coingecko.com/api/v3/coins/ethereum/contract/0x7DD9c5Cba05E151C895FDe1CF355C9A1D5DA6429"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        price = str(data["market_data"]["current_price"]["usd"])[0:5]
        obj, created = GLM.objects.get_or_create(id=1)
        obj.current_price = price
        obj.save()
    else:
        print("Failed to retrieve data")


import asyncio
from .scanner import monitor_nodes_status


@app.task
def v2_offer_scraper(subnet_tag="public"):
    # Run the asyncio function using asyncio.run()
    asyncio.run(monitor_nodes_status(subnet_tag))


@app.task(queue="yagna")
def healthcheck_provider(node_id, network, taskId):
    command = f"cd /stats-backend/healthcheck && npm i && node start.mjs {node_id} {network} {taskId}"
    with subprocess.Popen(
        command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
    ) as proc:
        while True:
            output = proc.stdout.readline()
            if output == "" and proc.poll() is not None:
                break
            if output:
                print(output.strip())

    rc = proc.poll()
    return rc


@app.task
def store_ec2_info():
    ec2_info = {}
    products_data = get_ec2_products()

    for product in products_data:
        details = product.get("details", {})
        if not has_vcpu_memory(details):
            continue
        print(product)
        product_id = product["id"]
        category = product.get("category")
        name = product.get("name")

        pricing_data = get_pricing(product_id)
        cheapest_price = find_cheapest_price(pricing_data["prices"])

        # Convert memory to float and price to Decimal
        memory_gb = float(details["memory"])
        price = cheapest_price["amount"] if cheapest_price else None

        # Use get_or_create to store or update the instance in the database
        instance, created = EC2Instance.objects.get_or_create(
            name=name,
            defaults={"vcpu": details["vcpu"], "memory": memory_gb, "price_usd": price},
        )

        ec2_info[product_id] = {
            "category": category,
            "name": name,
            "details": details,
            "cheapest_price": cheapest_price,
        }

    return ec2_info
