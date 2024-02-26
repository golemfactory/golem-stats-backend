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
from collector.models import NetworkStats, ProvidersComputingMax
from django.db.models import Avg, Max
from datetime import datetime, timedelta

from django.db.models import (
    FloatField,
)
from django.db.models.functions import Cast


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)


@app.task
def compare_ec2_and_golem():
    try:
        ec2_instances = EC2Instance.objects.all()
        comparison_results = []

        for ec2 in ec2_instances:
            cheapest_offer = (
                Offer.objects.annotate(
                    vcpu=Cast("properties__golem.inf.cpu.threads", FloatField()),
                    memory=Cast("properties__golem.inf.mem.gib", FloatField()),
                )
                .filter(
                    runtime="vm",
                    vcpu=ec2.vcpu,
                    memory__gte=ec2.memory,
                    provider__online=True,
                )
                .order_by("hourly_price_usd")
                .first()
            )

            if (
                cheapest_offer
                and cheapest_offer.hourly_price_usd
                and ec2.price_usd
                and ec2.price_usd > 0
            ):
                percentage_cheaper = (
                    (float(ec2.price_usd) - cheapest_offer.hourly_price_usd)
                    / float(ec2.price_usd)
                ) * 100
                node_id = cheapest_offer.provider.node_id
            else:
                percentage_cheaper = node_id = None

            comparison_results.append(
                {
                    "ec2_instance_name": ec2.name,
                    "ec2_vcpu": ec2.vcpu,
                    "ec2_memory": ec2.memory,
                    "ec2_hourly_price_usd": ec2.price_usd,
                    "cheapest_golem_hourly_price_usd": (
                        cheapest_offer.hourly_price_usd if cheapest_offer else None
                    ),
                    "golem_node_id": node_id,
                    "golem_percentage_cheaper": (
                        round(percentage_cheaper, 2)
                        if percentage_cheaper is not None
                        else None
                    ),
                }
            )

        r.set("ec2_comparison", json.dumps(comparison_results, cls=DecimalEncoder))
    except Exception as e:
        print(f"Error: {e}")


@app.task
def network_historical_stats_to_redis_v2():
    now = datetime.now()
    one_day_ago = now - timedelta(days=1)
    seven_days_ago = now - timedelta(days=7)
    one_month_ago = now - timedelta(days=30)
    one_year_ago = now - timedelta(days=365)

    def data_with_granularity(start_date, end_date, granularity):
        stats = (
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
        if granularity in [TruncDay, TruncHour]:
            for stat in stats:
                stat_date = stat["timestamp"]
                computing_total = (
                    ProvidersComputingMax.objects.filter(
                        date__date=stat_date
                    ).aggregate(Max("total"))["total__max"]
                    or 0
                )
                stat["computing"] = computing_total
        return stats

    hourly_data_past_day = data_with_granularity(one_day_ago, now, TruncHour)
    daily_data_past_7_days = data_with_granularity(seven_days_ago, now, TruncDay)
    daily_data_past_month = data_with_granularity(one_month_ago, now, TruncDay)
    daily_data_past_year = data_with_granularity(one_year_ago, now, TruncDay)

    formatted_data = {"1d": [], "7d": [], "1m": [], "1y": [], "All": []}

    def append_data(data_source, key):
        for entry in data_source:
            formatted_entry = {
                "date": entry["timestamp"].timestamp(),
                "online": round(entry["online"]),
                "cores": round(entry["cores"]),
                "memory": round(entry["memory"] / 1024, 2),
                "disk": round(entry["disk"] / 1024, 2),
            }
            if "computing" in entry:
                formatted_entry["computing"] = entry["computing"]
            formatted_data[key].append(formatted_entry)

    append_data(hourly_data_past_day, "1d")
    append_data(daily_data_past_7_days, "7d")
    append_data(daily_data_past_month, "1m")
    append_data(daily_data_past_year, "1y")

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


import time
from api.utils import get_stats_data
from .models import ProviderWithTask, Node, Offer, PricingSnapshot
from .utils import identify_network_by_offer


@app.task
def providers_who_received_tasks():
    now = round(time.time())
    domain = (
        os.environ.get("STATS_URL")
        + f"api/datasources/proxy/40/api/v1/query_range?query=increase(payment_invoices_provider_accepted%7Bjob%3D%22community.1%22%7D%5B10m%5D)%20%3E%200&start={now}&end={now}&step=5"
    )
    content, status_code = get_stats_data(domain)
    if status_code == 200:
        data = content["data"]["result"]
        for obj in data:
            instance_id = obj["metric"]["instance"]
            node, _ = Node.objects.get_or_create(node_id=instance_id)
            try:
                offer = Offer.objects.get(provider=node, runtime="vm")
                if offer is None:
                    continue
                print(offer.properties, "offer is here")
                pricing_model = offer.properties.get(
                    "golem.com.pricing.model.linear.coeffs", []
                )
                usage_vector = offer.properties.get("golem.com.usage.vector", [])
                if not usage_vector or not pricing_model:
                    continue

                static_start_price = pricing_model[-1]

                cpu_index = usage_vector.index("golem.usage.cpu_sec")
                cpu_per_hour_price = pricing_model[cpu_index] * 3600

                duration_index = usage_vector.index("golem.usage.duration_sec")
                env_per_hour_price = pricing_model[duration_index] * 3600

                ProviderWithTask.objects.create(
                    instance=node,
                    offer=offer,
                    cpu_per_hour=cpu_per_hour_price,
                    env_per_hour=env_per_hour_price,
                    start_price=static_start_price,
                    network=identify_network_by_offer(offer),
                )
            except Offer.DoesNotExist:
                print(f"Offer for node {node.node_id} not found")
                pass  # If no VM runtime offer found for node, continue with next


from django.db.models import Avg
from numpy import median


@app.task
def create_pricing_snapshot():
    try:
        last_24_hours = timezone.now() - timedelta(days=1)
        data_date = last_24_hours.date()  # Store the date when the data was collected
        cpu_prices = ProviderWithTask.objects.filter(
            created_at__gte=last_24_hours
        ).values_list("cpu_per_hour", flat=True)
        env_prices = ProviderWithTask.objects.filter(
            created_at__gte=last_24_hours
        ).values_list("env_per_hour", flat=True)
        start_prices = ProviderWithTask.objects.filter(
            created_at__gte=last_24_hours
        ).values_list("start_price", flat=True)

        cpu_prices_cleaned = [price for price in cpu_prices if price is not None]
        env_prices_cleaned = [price for price in env_prices if price is not None]
        start_prices_cleaned = [price for price in start_prices if price is not None]

        snapshot = PricingSnapshot(
            average_cpu_price=(
                sum(cpu_prices_cleaned) / len(cpu_prices_cleaned)
                if cpu_prices_cleaned
                else 0
            ),
            median_cpu_price=median(cpu_prices_cleaned) if cpu_prices_cleaned else 0,
            average_env_price=(
                sum(env_prices_cleaned) / len(env_prices_cleaned)
                if env_prices_cleaned
                else 0
            ),
            median_env_price=median(env_prices_cleaned) if env_prices_cleaned else 0,
            average_start_price=(
                sum(start_prices_cleaned) / len(start_prices_cleaned)
                if start_prices_cleaned
                else 0
            ),
            median_start_price=(
                median(start_prices_cleaned) if start_prices_cleaned else 0
            ),
            created_at=timezone.now(),
            date=data_date,
        )
        snapshot.save()
    except Exception as e:
        print(e)  # Replace with actual logging


@app.task
def median_and_average_pricing_past_hour():
    try:
        last_hour = timezone.now() - timedelta(hours=1)
        cpu_values = ProviderWithTask.objects.filter(created_at__gte=last_hour).exclude(
            cpu_per_hour__isnull=True
        )
        env_values = ProviderWithTask.objects.filter(created_at__gte=last_hour).exclude(
            env_per_hour__isnull=True
        )
        start_values = ProviderWithTask.objects.filter(
            created_at__gte=last_hour
        ).exclude(start_price__isnull=True)

        cpu_median = median(cpu_values.values_list("cpu_per_hour", flat=True))
        cpu_average = cpu_values.aggregate(Avg("cpu_per_hour"))["cpu_per_hour__avg"]

        env_median = median(env_values.values_list("env_per_hour", flat=True))
        env_average = env_values.aggregate(Avg("env_per_hour"))["env_per_hour__avg"]

        start_median = median(start_values.values_list("start_price", flat=True))
        start_average = start_values.aggregate(Avg("start_price"))["start_price__avg"]

        pricing_data = {
            "cpu_median": cpu_median,
            "cpu_average": cpu_average,
            "env_median": env_median,
            "env_average": env_average,
            "start_median": start_median,
            "start_average": start_average,
        }
        print(f"Median and average pricing data: {pricing_data}")

        r.set("pricing_past_hour_v2", json.dumps(pricing_data))
    except Exception as e:
        print(e)  # Replace with proper logging mechanism


import numpy as np


@app.task
def chart_pricing_data_for_frontend():
    def pricing_snapshot_stats_with_dates(start_date, end_date):
        snapshot_data = PricingSnapshot.objects.filter(
            created_at__range=(start_date, end_date)
        ).order_by("created_at")
        data = []
        for snapshot in snapshot_data:
            data_entry = {
                "date": snapshot.created_at.timestamp(),
                "average_cpu": snapshot.average_cpu_price,
                "median_cpu": snapshot.median_cpu_price,
                "average_env": snapshot.average_env_price,
                "median_env": snapshot.median_env_price,
                "average_start": snapshot.average_start_price,
                "median_start": snapshot.median_start_price,
            }
            data.append(data_entry)
        return data

    now = datetime.now()
    seven_days_ago = now - timedelta(days=7)
    one_month_ago = now - timedelta(days=30)
    six_months_ago = now - timedelta(days=30 * 6)
    one_year_ago = now - timedelta(days=365)

    data = {
        "7d": pricing_snapshot_stats_with_dates(seven_days_ago, now),
        "1m": pricing_snapshot_stats_with_dates(one_month_ago, now),
        "6m": pricing_snapshot_stats_with_dates(six_months_ago, now),
        "1y": pricing_snapshot_stats_with_dates(one_year_ago, now),
        "All": pricing_snapshot_stats_with_dates(
            PricingSnapshot.objects.earliest("created_at").created_at, now
        ),
    }

    r.set("pricing_data_charted_v2", json.dumps(data))
