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
from django.core.serializers.json import DjangoJSONEncoder

from decimal import Decimal
from .utils import (
    identify_network_by_offer,
    is_provider_online,
    process_and_store_product_data,
    extract_pricing_from_vm_properties,
    identify_network,
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

from .scoring import calculate_uptime_percentage
from django.db import transaction


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)


@app.task
def online_nodes_uptime_donut_data():

    try:
        # Fetching nodes with only necessary fields to reduce query load
        nodes_mainnet = Node.objects.filter(online=True, network="mainnet").only(
            "node_id"
        )
        nodes_testnet = Node.objects.filter(online=True, network="testnet").only(
            "node_id"
        )

        # Initializing data structure for each network
        uptime_data = {
            "mainnet": {
                "80_and_over": 0,
                "50_to_79": 0,
                "30_to_49": 0,
                "below_30": 0,
                "totalOnline": nodes_mainnet.count(),
            },
            "testnet": {
                "80_and_over": 0,
                "50_to_79": 0,
                "30_to_49": 0,
                "below_30": 0,
                "totalOnline": nodes_testnet.count(),
            },
        }

        def update_uptime_data(nodes, network):
            for node in nodes:
                uptime_percentage = calculate_uptime_percentage(node.node_id)
                if uptime_percentage >= 80:
                    uptime_data[network]["80_and_over"] += 1
                elif 50 <= uptime_percentage < 80:
                    uptime_data[network]["50_to_79"] += 1
                elif 30 <= uptime_percentage < 50:
                    uptime_data[network]["30_to_49"] += 1
                else:
                    uptime_data[network]["below_30"] += 1

        # Updating uptime data for each network
        update_uptime_data(nodes_mainnet, "mainnet")
        update_uptime_data(nodes_testnet, "testnet")

        # Save the result in a cache or similar storage
        r.set("online_nodes_uptime_donut_data", json.dumps(uptime_data))
    except Exception as e:
        print(f"Error: {e}")


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
                    provider__network="mainnet",
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
    now = timezone.now()
    runtime_names = NetworkStats.objects.values_list("runtime", flat=True).distinct()
    formatted_data = {
        runtime: {"1d": [], "7d": [], "1m": [], "1y": [], "All": []}
        for runtime in runtime_names
    }

    def data_with_granularity(runtime_name, start_date, end_date, granularity):
        stats = (
            NetworkStats.objects.filter(
                runtime=runtime_name, date__range=(start_date, end_date)
            )
            .annotate(timestamp=granularity("date"))
            .values("timestamp")
            .annotate(
                online=Avg("online"),
                cores=Avg("cores"),
                memory=Avg("memory"),
                disk=Avg("disk"),
                gpus=Avg("gpus"),
            )
            .order_by("timestamp")
        )
        if granularity in [TruncDay, TruncHour]:
            latest_stat = (
                NetworkStats.objects.filter(runtime=runtime_name, date__lt=end_date)
                .order_by("-date")
                .values("online", "cores", "memory", "disk", "gpus")
                .first()
            )
            if latest_stat:
                latest_stat["timestamp"] = end_date - timedelta(microseconds=1)
                stats = list(stats) + [latest_stat]
        return stats

    def append_data(formatted_data, runtime_name, data_source, key):
        for entry in data_source:
            formatted_entry = {
                "date": entry["timestamp"].timestamp(),
                "online": round(entry.get("online", 0)),
                "cores": round(entry.get("cores", 0)),
                "memory": round(entry.get("memory", 0) / 1024, 2),
                "disk": round(entry.get("disk", 0) / 1024, 2),
                "gpus": round(entry.get("gpus", 0)),
            }
            formatted_data[runtime_name][key].append(formatted_entry)

    for runtime_name in runtime_names:
        time_intervals = [
            ("1d", now - timedelta(days=1)),
            ("7d", now - timedelta(days=7)),
            ("1m", now - timedelta(days=30)),
            ("1y", now - timedelta(days=365)),
            (
                "All",
                NetworkStats.objects.filter(runtime=runtime_name).earliest("date").date,
            ),
        ]
        for key, start_date in time_intervals:
            data = data_with_granularity(
                runtime_name,
                start_date,
                now,
                TruncDay if key not in ["1d"] else TruncHour,
            )
            append_data(formatted_data, runtime_name, data, key)

    r.set("network_historical_stats_v2", json.dumps(formatted_data))


@app.task
def v2_network_online_to_redis():
    # Fetch and process data from the external domain
    response = requests.get(
        "https://reputation.dev-test.golem.network/v2/providers/scores"
    )
    if response.status_code == 200:
        external_data = response.json()

        # Mapping of providerId to successRate
        success_rate_mapping = {
            provider["provider"]["id"]: provider["scores"]["successRate"]
            for provider in external_data["testedProviders"]
        }

        # Mapping of blacklisted providerId to the reason
        blacklist_provider_mapping = {
            provider["provider"]["id"]: provider["reason"]
            for provider in external_data["rejectedProviders"]
        }

        # Mapping of blacklisted operator walletAddress to the reason
        blacklist_operator_mapping = {
            operator["operator"]["walletAddress"]: operator["reason"]
            for operator in external_data["rejectedOperators"]
        }

        # Fetch your existing nodes
        data = Node.objects.filter(online=True)
        serializer = NodeSerializer(data, many=True)
        serialized_data = serializer.data

        # Attach successRate and blacklist status to each node
        for node in serialized_data:
            node_id = node["node_id"]
            wallet = node.get("wallet")  # Assuming 'wallet' attribute exists

            node["reputation"] = {}
            node["reputation"]["blacklisted"] = False
            node["reputation"]["blacklistedReason"] = None

            if node_id in blacklist_provider_mapping:
                node["reputation"]["blacklisted"] = True
                node["reputation"]["blacklistedReason"] = blacklist_provider_mapping[
                    node_id
                ]
            elif wallet in blacklist_operator_mapping:
                node["reputation"]["blacklisted"] = True
                node["reputation"]["blacklistedReason"] = blacklist_operator_mapping[
                    wallet
                ]

            if node_id in success_rate_mapping:
                node["reputation"]["taskReputation"] = (
                    success_rate_mapping[node_id] * 100
                )
            else:
                node["reputation"]["taskReputation"] = None

        # Serialize and save to Redis
        test = json.dumps(serialized_data, default=str)
        r.set("v2_online", test)
    else:
        print(
            "Failed to retrieve data from the reputation system!", response.status_code
        )
        pass


@app.task
def v2_network_online_to_redis_new_stats_page(runtime=None):
    try:
        response = requests.get(
            "https://reputation.dev-test.golem.network/v1/providers/scores"
        )
        response.raise_for_status()
        external_data = response.json()
        success_rate_mapping = {
            provider["providerId"]: provider["scores"]["successRate"]
            for provider in external_data["providers"]
        }

        filters = {"online": True}
        if runtime:
            filters["offer__runtime"] = runtime

        data = Node.objects.filter(**filters).order_by("node_id").distinct()
        serializer = NodeSerializer(data, many=True)
        serialized_data = serializer.data
        size = 30
        page_key_suffix = f"_{runtime}" if runtime else ""

        total_pages = (len(serialized_data) // size) + (
            0 if len(serialized_data) % size == 0 else 1
        )
        for page in range(1, total_pages + 1):
            paginated_data = serialized_data[(page - 1) * size : page * size]
            for node in paginated_data:
                node_id = node["node_id"]
                node["taskReputation"] = success_rate_mapping.get(node_id, None)
            r.set(
                f"v2_online_{page}_{size}{page_key_suffix}",
                json.dumps(paginated_data, default=str),
            )
        r.set(
            f"v2_online_metadata{page_key_suffix}",
            json.dumps({"total_pages": total_pages, "size": size}),
        )
    except requests.HTTPError as e:
        print(f"Failed to retrieve data: {e}")


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
        if not "node_id" in obj["properties"]:
            continue
        provider = {}
        provider["name"] = "Golem Network"
        provider["node_id"] = obj["properties"]["node_id"]
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


@app.task(
    bind=True,
    autoretry_for=(requests.exceptions.RequestException,),
    retry_backoff=True,
    retry_jitter=True,
    retry_kwargs={"max_retries": 5},
)
def store_ec2_info(self):
    url = "https://api.vantage.sh/v2/products?service_id=aws-ec2"
    headers = {
        "accept": "application/json",
        "authorization": f'Bearer {os.environ.get("VANTAGE_API_KEY")}',
    }
    try:
        response = requests.get(url, headers=headers)
        # Check for rate limiting error
        if response.status_code == 429:
            reset_time = int(response.headers.get("x-rate-limit-reset", 0))
            current_time = time.time()
            retry_after = max(
                reset_time - current_time, 1
            )  # Ensure there's at least a 1-second wait
            # Schedule the next retry to align with the rate limit reset time
            raise self.retry(countdown=retry_after)
        response.raise_for_status()

        products_data = response.json().get("products", [])
        for product in products_data:
            process_and_store_product_data.delay(product)
    except requests.RequestException as exc:
        # Reraise with self.retry to utilize Celery's built-in retry mechanism
        raise self.retry(exc=exc)


import time
from api.utils import get_stats_data
from .models import ProviderWithTask, Node, Offer, PricingSnapshot
from .utils import identify_network_by_offer


@app.task
def v2_network_stats_to_redis():

    stats_by_runtime = {}

    vm_offers_query = Offer.objects.filter(provider__online=True)

    for offer in vm_offers_query:
        properties = offer.properties
        if not properties:
            print(f"Offer {offer.id} has no properties {offer}")
            continue
        runtime_name = offer.runtime

        if runtime_name not in stats_by_runtime:
            stats_by_runtime[runtime_name] = {
                "online": 0,
                "cores": 0,
                "threads": 0,
                "memory": 0.0,
                "disk": 0.0,
                "gpus": 0,
                "cuda_cores": 0,
                "gpu_memory": 0.0,
                "gpu_models": {},
            }
        stats = stats_by_runtime[runtime_name]
        stats["online"] += 1
        stats["cores"] += properties.get("golem.inf.cpu.cores", 0)
        stats["threads"] += properties.get("golem.inf.cpu.threads", 0)
        stats["memory"] += properties.get("golem.inf.mem.gib", 0.0)
        stats["disk"] += properties.get("golem.inf.storage.gib", 0.0)
        gpu = properties.get("golem.!exp.gap-35.v1.inf.gpu.model", None)
        if gpu:
            stats["gpus"] += 1
            stats["cuda_cores"] += properties.get(
                "golem.!exp.gap-35.v1.inf.gpu.cuda.cores", 0
            )
            stats["gpu_memory"] += properties.get(
                "golem.!exp.gap-35.v1.inf.gpu.memory.total.gib", 0.0
            )
            stats["gpu_models"][gpu] = stats["gpu_models"].get(gpu, 0) + 1

    total_online_mainnet = Node.objects.filter(online=True, network="mainnet").count()
    total_online_testnet = Node.objects.filter(online=True, network="testnet").count()
    stats_by_runtime["totalOnlineMainnet"] = total_online_mainnet
    stats_by_runtime["totalOnlineTestnet"] = total_online_testnet

    serialized = json.dumps(stats_by_runtime, default=dict)
    r.set("online_stats_by_runtime", serialized)

    for runtime, data in stats_by_runtime.items():
        if isinstance(data, dict):
            NetworkStats.objects.create(
                online=data["online"],
                cores=data["cores"],
                memory=data["memory"],
                disk=data["disk"],
                runtime=runtime,
                gpus=data.get("gpus", 0),
                cuda_cores=data.get("cuda_cores", 0),
                gpu_memory=data.get("gpu_memory", 0.0),
                gpu_models=data.get("gpu_models", {}),
            )


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


from django.db.models import Avg
from numpy import median


@app.task
def create_pricing_snapshot(network):
    try:
        last_24_hours = timezone.now() - timedelta(days=1)
        data_date = last_24_hours.date()  # Store the date when the data was collected
        cpu_prices = ProviderWithTask.objects.filter(
            created_at__gte=last_24_hours, network=network
        ).values_list("cpu_per_hour", flat=True)
        env_prices = ProviderWithTask.objects.filter(
            created_at__gte=last_24_hours, network=network
        ).values_list("env_per_hour", flat=True)
        start_prices = ProviderWithTask.objects.filter(
            created_at__gte=last_24_hours, network=network
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
            date=last_24_hours,
            network=network,
        )
        snapshot.save()
    except Exception as e:
        print(e)  # Replace with actual logging


from django.db.models import Q


@app.task
def median_and_average_pricing_past_hour():
    try:
        last_hour = timezone.now() - timedelta(hours=1)
        filters = Q(created_at__gte=last_hour) & (
            Q(network="testnet") | Q(network="mainnet")
        )

        cpu_values = ProviderWithTask.objects.filter(filters).exclude(
            cpu_per_hour__isnull=True
        )
        env_values = ProviderWithTask.objects.filter(filters).exclude(
            env_per_hour__isnull=True
        )
        start_values = ProviderWithTask.objects.filter(filters).exclude(
            start_price__isnull=True
        )

        cpu_median_testnet = median(
            cpu_values.filter(network="testnet").values_list("cpu_per_hour", flat=True)
        )
        cpu_average_testnet = cpu_values.filter(network="testnet").aggregate(
            Avg("cpu_per_hour")
        )["cpu_per_hour__avg"]
        cpu_median_mainnet = median(
            cpu_values.filter(network="mainnet").values_list("cpu_per_hour", flat=True)
        )
        cpu_average_mainnet = cpu_values.filter(network="mainnet").aggregate(
            Avg("cpu_per_hour")
        )["cpu_per_hour__avg"]

        env_median_testnet = median(
            env_values.filter(network="testnet").values_list("env_per_hour", flat=True)
        )
        env_average_testnet = env_values.filter(network="testnet").aggregate(
            Avg("env_per_hour")
        )["env_per_hour__avg"]
        env_median_mainnet = median(
            env_values.filter(network="mainnet").values_list("env_per_hour", flat=True)
        )
        env_average_mainnet = env_values.filter(network="mainnet").aggregate(
            Avg("env_per_hour")
        )["env_per_hour__avg"]

        start_median_testnet = median(
            start_values.filter(network="testnet").values_list("start_price", flat=True)
        )
        start_average_testnet = start_values.filter(network="testnet").aggregate(
            Avg("start_price")
        )["start_price__avg"]
        start_median_mainnet = median(
            start_values.filter(network="mainnet").values_list("start_price", flat=True)
        )
        start_average_mainnet = start_values.filter(network="mainnet").aggregate(
            Avg("start_price")
        )["start_price__avg"]

        pricing_data = {
            "testnet": {
                "cpu_median": cpu_median_testnet if cpu_median_testnet else 0,
                "cpu_average": cpu_average_testnet if cpu_average_testnet else 0,
                "env_median": env_median_testnet if env_median_testnet else 0,
                "env_average": env_average_testnet if env_average_testnet else 0,
                "start_median": start_median_testnet if start_median_testnet else 0,
                "start_average": start_average_testnet if start_average_testnet else 0,
            },
            "mainnet": {
                "cpu_median": cpu_median_mainnet if cpu_median_mainnet else 0,
                "cpu_average": cpu_average_mainnet if cpu_average_mainnet else 0,
                "env_median": env_median_mainnet if env_median_mainnet else 0,
                "env_average": env_average_mainnet if env_average_mainnet else 0,
                "start_median": start_median_mainnet if start_median_mainnet else 0,
                "start_average": start_average_mainnet if start_average_mainnet else 0,
            },
        }

        r.set("pricing_past_hour_v2", json.dumps(pricing_data))
    except Exception as e:
        print(e)  # Replace with proper logging mechanism


import numpy as np


@app.task
def chart_pricing_data_for_frontend():
    def pricing_snapshot_stats_with_dates(start_date, end_date, network):
        snapshot_data = PricingSnapshot.objects.filter(
            created_at__range=(start_date, end_date), network=network
        ).order_by("date")

        data = []
        for snapshot in snapshot_data:
            if snapshot.date is not None:
                data.append(
                    {
                        "date": snapshot.date.timestamp(),
                        "average_cpu": snapshot.average_cpu_price,
                        "median_cpu": snapshot.median_cpu_price,
                        "average_env": snapshot.average_env_price,
                        "median_env": snapshot.median_env_price,
                        "average_start": snapshot.average_start_price,
                        "median_start": snapshot.median_start_price,
                    }
                )
        return data

    now = datetime.now()
    seven_days_ago = now - timedelta(days=7)
    one_month_ago = now - timedelta(days=30)
    six_months_ago = now - timedelta(days=30 * 6)
    one_year_ago = now - timedelta(days=365)

    networks_data = {}
    for network in ["testnet", "mainnet"]:
        data = {
            "7d": pricing_snapshot_stats_with_dates(seven_days_ago, now, network),
            "1m": pricing_snapshot_stats_with_dates(one_month_ago, now, network),
            "6m": pricing_snapshot_stats_with_dates(six_months_ago, now, network),
            "1y": pricing_snapshot_stats_with_dates(one_year_ago, now, network),
            "All": pricing_snapshot_stats_with_dates(
                PricingSnapshot.objects.filter(network=network)
                .earliest("created_at")
                .created_at,
                now,
                network,
            ),
        }
        networks_data[network] = data

    r.set("pricing_data_charted_v2", json.dumps(networks_data))


from django.db.models import Max, Sum
from django.db.models import (
    Count,
    Avg,
    StdDev,
    FloatField,
    Q,
    Subquery,
    OuterRef,
    F,
    Case,
    When,
    Max,
)
from django.db.models.functions import Cast
from datetime import timedelta
from django.db.models import Subquery, OuterRef
from django.db.models.fields.json import KeyTextTransform
from django.db import models
from django.db.models import IntegerField, FloatField


@app.task
def sum_highest_runtime_resources():
    online_nodes = Node.objects.filter(online=True)

    total_cores = 0
    total_memory = 0
    total_storage = 0
    total_gpus = 0

    for node in online_nodes:
        offers = Offer.objects.filter(provider=node)
        max_resources = offers.annotate(
            cores=Cast(
                KeyTextTransform("golem.inf.cpu.threads", "properties"),
                IntegerField(),
            ),
            memory=Cast(
                KeyTextTransform("golem.inf.mem.gib", "properties"), FloatField()
            ),
            storage=Cast(
                KeyTextTransform("golem.inf.storage.gib", "properties"),
                FloatField(),
            ),
            gpu_model=KeyTextTransform(
                "golem.!exp.gap-35.v1.inf.gpu.model", "properties"
            ),
        ).aggregate(
            max_cores=Max("cores"),
            max_memory=Max("memory"),
            max_storage=Max("storage"),
            gpu_count=Count("gpu_model", filter=Q(gpu_model__isnull=False)),
        )

        total_cores += max_resources["max_cores"] if max_resources["max_cores"] else 0
        total_memory += (
            max_resources["max_memory"] if max_resources["max_memory"] else 0
        )
        total_storage += (
            max_resources["max_storage"] if max_resources["max_storage"] else 0
        )
        total_gpus += max_resources["gpu_count"]

    print(
        f"Total cores: {total_cores}"
        f"Total memory: {total_memory}"
        f"Total storage: {total_storage}"
        f"Total gpus: {total_gpus}"
    )
    r.set(
        "v2_network_online_stats",
        json.dumps(
            {
                "providers": online_nodes.count(),
                "cores": total_cores,
                "memory": total_memory,
                "storage": total_storage,
                "gpus": total_gpus,
            }
        ),
    )


from django.db.models import Count, F, Window
from django.db.models.functions import Lag, TruncHour
from django.utils import timezone
from .models import NodeStatusHistory


@app.task
def get_online_counts():
    last_24_hours = timezone.now() - timedelta(hours=24)
    data_points = (
        NodeStatusHistory.objects.filter(timestamp__gte=last_24_hours, is_online=True)
        .annotate(hour=TruncHour("timestamp"))
        .values("hour")
        .annotate(online_count=Count("provider", distinct=True))
        .annotate(
            prev_count=Window(
                expression=Lag("online_count", default=None), order_by=F("hour").asc()
            )
        )
        .order_by("hour")[:90]
    )

    formatted_data = [
        {"date": point["hour"].timestamp(), "providers": point["online_count"]}
        for point in data_points
    ]

    last_count = formatted_data[-1]["providers"] if formatted_data else 0
    first_count = formatted_data[0]["providers"] if formatted_data else 0

    change = last_count - first_count
    percentage_change = ((change / first_count) * 100) if first_count else 0
    change_type = "positive" if change >= 0 else "negative"

    stats = {
        "change": f"{change}",
        "percentageChange": f"{percentage_change:.2f}%",
        "changeType": change_type,
        "value": Node.objects.filter(online=True).count(),
    }

    result = {"data": formatted_data, "stats": stats}
    r.set("v2_online_counts", json.dumps(result))


from django.db.models import CharField, Value
from django.db.models.functions import Cast, Replace
from collections import defaultdict
import json


@app.task
def count_cpu_vendors():
    # Ensure the correct output field is set to CharField to avoid mixed types error.
    online_nodes_offers = (
        Offer.objects.filter(
            provider__online=True, properties__has_key="golem.inf.cpu.vendor"
        )
        .annotate(
            clean_cpu_vendor=Replace(
                Cast("properties__golem.inf.cpu.vendor", CharField()),
                Value('"'),
                Value(""),
            )
        )
        .values("provider_id", "clean_cpu_vendor")
        .distinct()
    )

    cpu_vendors_count = defaultdict(int)
    processed_nodes = set()

    for offer in online_nodes_offers:
        node_id = offer["provider_id"]
        if node_id not in processed_nodes:
            cpu_vendors_count[offer["clean_cpu_vendor"]] += 1
            processed_nodes.add(node_id)

    cpu_vendors_json = json.dumps(cpu_vendors_count)

    r.set("cpu_vendors_count", cpu_vendors_json)


@app.task
def count_cpu_architecture():
    # Ensure the correct output field is set to CharField to avoid mixed types error.
    online_nodes_offers = (
        Offer.objects.filter(
            provider__online=True, properties__has_key="golem.inf.cpu.architecture"
        )
        .annotate(
            clean_cpu_architecture=Replace(
                Cast("properties__golem.inf.cpu.architecture", CharField()),
                Value('"'),
                Value(""),
            )
        )
        .values("provider_id", "clean_cpu_architecture")
        .distinct()
    )

    cpu_architecture_count = defaultdict(int)
    cpu_architecture_count["arm64"] = 0
    processed_nodes = set()

    for offer in online_nodes_offers:
        node_id = offer["provider_id"]
        if node_id not in processed_nodes:
            cpu_architecture_count[offer["clean_cpu_architecture"]] += 1
            processed_nodes.add(node_id)

    cpu_architecture_json = json.dumps(cpu_architecture_count)

    r.set("cpu_architecture_count", cpu_architecture_json)


import urllib.parse


@app.task
def online_nodes_computing():
    end = round(time.time())
    start = end - 10
    query = 'activity_provider_created{job="community.1"} - activity_provider_destroyed{job="community.1"}'
    url = f"{os.environ.get('STATS_URL')}api/datasources/proxy/40/api/v1/query_range?query={urllib.parse.quote(query)}&start={start}&end={end}&step=1"
    data = get_stats_data(url)

    if data[1] == 200 and data[0]["status"] == "success" and data[0]["data"]["result"]:
        computing_node_ids = [
            node["metric"]["instance"]
            for node in data[0]["data"]["result"]
            if node["values"][-1][1] == "1"
        ]
        Node.objects.filter(node_id__in=computing_node_ids).update(computing_now=True)
        Node.objects.exclude(node_id__in=computing_node_ids).update(computing_now=False)
        NodeV1.objects.filter(node_id__in=computing_node_ids).update(computing_now=True)
        NodeV1.objects.exclude(node_id__in=computing_node_ids).update(
            computing_now=False
        )


from .models import RelayNodes


@app.task
def fetch_and_store_relay_nodes():
    base_url = "http://yacn2.dev.golem.network:9000/nodes/"

    for prefix in range(256):
        try:
            response = requests.get(f"{base_url}{prefix:02x}")
            response.raise_for_status()
            data = response.json()

            for node_id, sessions in data.items():
                node_id = node_id.strip().lower()
                ip_port = sessions[0]["peer"].split(":")
                ip, port = ip_port[0], int(ip_port[1])

                obj, created = RelayNodes.objects.update_or_create(
                    node_id=node_id, defaults={"ip_address": ip, "port": port}
                )

        except requests.RequestException as e:
            print(f"Error fetching data for prefix {prefix:02x}: {e}")


from .models import TransactionScraperIndex, GolemTransactions
from collector.models import Requestors
from django.utils.timezone import utc


@app.task
def init_golem_tx_scraping():
    index, _ = TransactionScraperIndex.objects.get_or_create(
        id=1, defaults={"indexed_before": False, "latest_timestamp_indexed": None}
    )

    current_time_epoch = int(datetime.utcnow().replace(tzinfo=utc).timestamp())

    start_epoch = 1553165313

    end_epoch = start_epoch + 2592000
    final_epoch = 1900313287

    URL_TEMPLATE = "http://erc20-api/erc20/api/stats/transfers?chain=137&receiver=all&from={}&to={}"

    try:
        latest_timestamp = 0
        while start_epoch < min(final_epoch, current_time_epoch):
            print(f"Fetching data from {start_epoch} to {end_epoch}")
            url = URL_TEMPLATE.format(
                start_epoch, min(end_epoch, final_epoch, current_time_epoch)
            )
            response = requests.get(url)
            if response.status_code != 200:
                print(url)
                raise Exception(
                    f"Failed to fetch data. Status code: {response.status_code}"
                )

            data = response.json()
            transfers = data.get("transfers", [])
            if not transfers:
                start_epoch = end_epoch + 1
                end_epoch += 2592000
                continue

            BATCH_SIZE = 5000
            from_addrs = {t["fromAddr"] for t in transfers}
            known_senders = set(
                Requestors.objects.filter(node_id__in=from_addrs).values_list(
                    "node_id", flat=True
                )
            ).union(
                RelayNodes.objects.filter(node_id__in=from_addrs).values_list(
                    "node_id", flat=True
                )
            )

            for i in range(0, len(transfers), BATCH_SIZE):
                with transaction.atomic():
                    batch = transfers[i : i + BATCH_SIZE]
                    golem_transactions = []
                    for t in batch:
                        timestamp = datetime.utcfromtimestamp(
                            t["blockTimestamp"]
                        ).replace(tzinfo=utc)
                        latest_timestamp = max(latest_timestamp, t["blockTimestamp"])
                        transaction_type = (
                            "batched"
                            if t["toAddr"]
                            == "0x50100d4faf5f3b09987dea36dc2eddd57a3e561b"
                            else (
                                "singleTransfer"
                                if t["toAddr"]
                                == "0x0b220b82f3ea3b7f6d9a1d8ab58930c064a2b5bf"
                                else None
                            )
                        )

                        golem_transactions.append(
                            GolemTransactions(
                                txhash=t["txHash"],
                                scanner_id=t["id"],
                                amount=int(t["tokenAmount"]) / 1e18,
                                timestamp=timestamp,
                                transaction_type=transaction_type,
                                receiver=t["receiverAddr"],
                                sender=t["fromAddr"],
                                tx_from_golem=t["fromAddr"] in known_senders,
                            )
                        )
                    GolemTransactions.objects.bulk_create(
                        golem_transactions, ignore_conflicts=True
                    )

            start_epoch = end_epoch + 1
            end_epoch += 2592000

        if current_time_epoch >= final_epoch or start_epoch >= current_time_epoch:
            index.indexed_before = True
            if latest_timestamp > 0:
                index.latest_timestamp_indexed = datetime.utcfromtimestamp(
                    latest_timestamp
                ).replace(tzinfo=utc)
            index.save()
    except Exception as e:
        raise e


@app.task
def fetch_latest_glm_tx():
    try:
        index = TransactionScraperIndex.objects.get(id=1)
        if not index.indexed_before or index.latest_timestamp_indexed is None:
            return
        epoch_now = int(timezone.now().timestamp())
        latest_timestamp = int(index.latest_timestamp_indexed.timestamp())
        url = f"http://erc20-api/erc20/api/stats/transfers?chain=137&receiver=all&from={latest_timestamp}&to={epoch_now}"
        response = requests.get(url)
        if response.status_code != 200:
            raise Exception(
                f"Failed to fetch updates. Status code: {response.status_code}"
            )

        data = response.json()
        print(data)
        transfers = data.get("transfers", [])
        if not transfers:
            print("No new transactions")
            return

        latest_block_timestamp = 0
        golem_transactions = []
        from_addrs = {t["fromAddr"] for t in transfers}
        known_senders = set(
            Requestors.objects.filter(node_id__in=from_addrs).values_list(
                "node_id", flat=True
            )
        )

        for t in transfers:
            timestamp = datetime.utcfromtimestamp(t["blockTimestamp"]).replace(
                tzinfo=timezone.utc
            )
            latest_block_timestamp = max(latest_block_timestamp, t["blockTimestamp"])
            if t["toAddr"] == "0x0b220b82f3ea3b7f6d9a1d8ab58930c064a2b5bf":
                transaction_type = "singleTransfer"
            elif t["toAddr"] == "0x50100d4faf5f3b09987dea36dc2eddd57a3e561b":
                transaction_type = "batched"
            else:
                transaction_type = None
            golem_transactions.append(
                GolemTransactions(
                    txhash=t["txHash"],
                    scanner_id=t["id"],
                    amount=int(t["tokenAmount"]) / 1e18,
                    transaction_type=transaction_type,
                    timestamp=timestamp,
                    receiver=t["receiverAddr"],
                    sender=t["fromAddr"],
                    tx_from_golem=t["fromAddr"] in known_senders,
                )
            )

        GolemTransactions.objects.bulk_create(golem_transactions, ignore_conflicts=True)
        index.latest_timestamp_indexed = datetime.utcfromtimestamp(
            latest_block_timestamp + 1
        ).replace(tzinfo=timezone.utc)
        index.save()
        print(f"New transactions added. Latest timestamp: {latest_block_timestamp}")
    except Exception as e:
        raise e


@app.task
def average_transaction_value_over_time():
    def aggregate_average_value(start_date, end_date):
        return (
            GolemTransactions.objects.filter(timestamp__range=(start_date, end_date))
            .annotate(date=TruncDay("timestamp"))
            .values("date")
            .annotate(
                on_golem=Coalesce(
                    Avg(
                        "amount",
                        filter=Q(tx_from_golem=True),
                        output_field=FloatField(),
                    ),
                    0,
                    output_field=FloatField(),
                ),
                not_golem=Coalesce(
                    Avg(
                        "amount",
                        filter=Q(tx_from_golem=False),
                        output_field=FloatField(),
                    ),
                    0,
                    output_field=FloatField(),
                ),
            )
            .order_by("date")
        )

    now = timezone.now()
    formatted_data = {
        "7d": [],
        "14d": [],
        "1m": [],
        "3m": [],
        "6m": [],
        "1y": [],
        "All": [],
    }
    intervals = {
        "7d": (now - timedelta(days=7), now),
        "14d": (now - timedelta(days=14), now),
        "1m": (now - timedelta(days=30), now),
        "3m": (now - timedelta(days=90), now),
        "6m": (now - timedelta(days=180), now),
        "1y": (now - timedelta(days=365), now),
        "All": (GolemTransactions.objects.earliest("timestamp").timestamp, now),
    }

    for period, (start_date, end_date) in intervals.items():
        data = aggregate_average_value(start_date, end_date)
        formatted_data[period] = list(data)

    r.set(
        "average_transaction_value_over_time",
        json.dumps(formatted_data, cls=DjangoJSONEncoder),
    )


from django.db.models import IntegerField, ExpressionWrapper, Case, When, Avg


@app.task
def daily_transaction_type_counts():
    def aggregate_counts(start_date, end_date):
        return (
            GolemTransactions.objects.filter(timestamp__range=(start_date, end_date))
            .annotate(date=TruncDay("timestamp"))
            .values("date")
            .annotate(
                singleTransfer=Count(
                    "scanner_id", filter=Q(transaction_type="singleTransfer")
                ),
                batched=Count("scanner_id", filter=Q(transaction_type="batched")),
            )
            .order_by("date")
        )

    now = timezone.now()
    formatted_data = {
        "7d": [],
        "14d": [],
        "1m": [],
        "3m": [],
        "6m": [],
        "1y": [],
        "All": [],
    }
    intervals = {
        "7d": (now - timedelta(days=7), now),
        "14d": (now - timedelta(days=14), now),
        "1m": (now - timedelta(days=30), now),
        "3m": (now - timedelta(days=90), now),
        "6m": (now - timedelta(days=180), now),
        "1y": (now - timedelta(days=365), now),
        "All": (GolemTransactions.objects.earliest("timestamp").timestamp, now),
    }

    for period, (start_date, end_date) in intervals.items():
        data = aggregate_counts(start_date, end_date)
        formatted_data[period] = list(data)

    r.set(
        "daily_transaction_type_counts",
        json.dumps(formatted_data, cls=DjangoJSONEncoder),
    )


@app.task
def transaction_type_comparison():
    now = timezone.now()
    formatted_data = {
        "7d": [],
        "14d": [],
        "1m": [],
        "3m": [],
        "6m": [],
        "1y": [],
        "All": [],
    }
    intervals = {
        "7d": (now - timedelta(days=7), now),
        "14d": (now - timedelta(days=14), now),
        "1m": (now - timedelta(days=30), now),
        "3m": (now - timedelta(days=90), now),
        "6m": (now - timedelta(days=180), now),
        "1y": (now - timedelta(days=365), now),
        "All": (GolemTransactions.objects.earliest("timestamp").timestamp, now),
    }

    def aggregate_types(start_date, end_date):
        return (
            GolemTransactions.objects.filter(
                timestamp__range=(start_date, end_date),
                transaction_type__in=["singleTransfer", "batched"],
            )
            .values("transaction_type")
            .annotate(total=Count("scanner_id"))
            .order_by("transaction_type")
        )

    for period, (start_date, end_date) in intervals.items():
        data = aggregate_types(start_date, end_date)
        formatted_data[period] = list(data)

    r.set(
        "transaction_type_comparison", json.dumps(formatted_data, cls=DjangoJSONEncoder)
    )


@app.task
def amount_transferred_over_time():
    def aggregate_amount(start_date, end_date):
        return (
            GolemTransactions.objects.filter(timestamp__range=(start_date, end_date))
            .annotate(date=TruncDay("timestamp"))
            .values("date")
            .annotate(total_amount=Sum("amount"))
            .order_by("date")
        )

    now = timezone.now()
    formatted_data = {
        "7d": [],
        "14d": [],
        "1m": [],
        "3m": [],
        "6m": [],
        "1y": [],
        "All": [],
    }
    intervals = {
        "7d": (now - timedelta(days=7), now),
        "14d": (now - timedelta(days=14), now),
        "1m": (now - timedelta(days=30), now),
        "3m": (now - timedelta(days=90), now),
        "6m": (now - timedelta(days=180), now),
        "1y": (now - timedelta(days=365), now),
        "All": (GolemTransactions.objects.earliest("timestamp").timestamp, now),
    }

    for period, (start_date, end_date) in intervals.items():
        data = aggregate_amount(start_date, end_date)
        formatted_data[period] = list(data)

    r.set(
        "amount_transferred_over_time",
        json.dumps(formatted_data, cls=DjangoJSONEncoder),
    )


@app.task
def transaction_volume_over_time():
    def aggregate_transactions(start_date, end_date):
        return (
            GolemTransactions.objects.filter(timestamp__range=(start_date, end_date))
            .annotate(date=TruncDay("timestamp"))
            .values("date")
            .annotate(
                on_golem=Count("amount", filter=Q(tx_from_golem=True), distinct=True),
                not_golem=Count("amount", filter=Q(tx_from_golem=False), distinct=True),
            )
            .order_by("date")
        )

    now = timezone.now()
    formatted_data = {
        "7d": [],
        "14d": [],
        "1m": [],
        "3m": [],
        "6m": [],
        "1y": [],
        "All": [],
    }
    intervals = {
        "7d": (now - timedelta(days=7), now),
        "14d": (now - timedelta(days=14), now),
        "1m": (now - timedelta(days=30), now),
        "3m": (now - timedelta(days=90), now),
        "6m": (now - timedelta(days=180), now),
        "1y": (now - timedelta(days=365), now),
        "All": (GolemTransactions.objects.earliest("timestamp").timestamp, now),
    }

    for period, (start_date, end_date) in intervals.items():
        data = aggregate_transactions(start_date, end_date)
        formatted_data[period] = list(data)

    r.set(
        "transaction_volume_over_time",
        json.dumps(formatted_data, cls=DjangoJSONEncoder),
    )


from django.db.models.functions import TruncDay, Coalesce


@app.task
def daily_volume_golem_vs_chain():
    def aggregate_volume(start_date, end_date):
        return (
            GolemTransactions.objects.filter(timestamp__range=(start_date, end_date))
            .annotate(date=TruncDay("timestamp"))
            .values("date")
            .annotate(
                on_golem=Coalesce(
                    Sum(
                        "amount",
                        filter=Q(tx_from_golem=True),
                        output_field=FloatField(),
                    ),
                    0,
                    output_field=FloatField(),
                ),
                not_golem=Coalesce(
                    Sum(
                        "amount",
                        filter=Q(tx_from_golem=False),
                        output_field=FloatField(),
                    ),
                    0,
                    output_field=FloatField(),
                ),
            )
            .order_by("date")
        )

    now = timezone.now()
    formatted_data = {
        "7d": [],
        "14d": [],
        "1m": [],
        "3m": [],
        "6m": [],
        "1y": [],
        "All": [],
    }
    intervals = {
        "7d": (now - timedelta(days=7), now),
        "14d": (now - timedelta(days=14), now),
        "1m": (now - timedelta(days=30), now),
        "3m": (now - timedelta(days=90), now),
        "6m": (now - timedelta(days=180), now),
        "1y": (now - timedelta(days=365), now),
        "All": (GolemTransactions.objects.earliest("timestamp").timestamp, now),
    }

    for period, (start_date, end_date) in intervals.items():
        data = aggregate_volume(start_date, end_date)
        formatted_data[period] = list(data)

    r.set(
        "daily_volume_golem_vs_chain", json.dumps(formatted_data, cls=DjangoJSONEncoder)
    )


from collector.models import ProvidersComputing


@app.task
def computing_total_over_time():
    now = timezone.now()
    formatted_data = {
        "7d": [],
        "14d": [],
        "1m": [],
        "3m": [],
        "6m": [],
        "1y": [],
        "All": [],
    }
    intervals = {
        "7d": (now - timedelta(days=7), now),
        "14d": (now - timedelta(days=14), now),
        "1m": (now - timedelta(days=30), now),
        "3m": (now - timedelta(days=90), now),
        "6m": (now - timedelta(days=180), now),
        "1y": (now - timedelta(days=365), now),
        "All": (ProvidersComputingMax.objects.earliest("date").date, now),
    }

    for period, (start_date, end_date) in intervals.items():
        data = (
            ProvidersComputingMax.objects.filter(date__range=(start_date, end_date))
            .annotate(truncated_date=TruncDay("date"))
            .values("truncated_date")
            .annotate(total=Sum("total"))
            .order_by("truncated_date")
        )
        formatted_data[period] = list(data)

    r.set(
        "computing_total_over_time", json.dumps(formatted_data, cls=DjangoJSONEncoder)
    )


@app.task
def extract_wallets_and_ids():
    from itertools import chain
    from collections import defaultdict
    from django.db.models import Q

    offers = Offer.objects.prefetch_related("provider").all()
    wallets_list = []
    providers_dict = defaultdict(list)

    for offer in offers:
        if not offer.properties:
            continue
        properties = offer.properties
        provider_id = properties.get("id", "")
        provider_name = properties.get("golem.node.id.name", "")
        wallets = [
            v
            for k, v in properties.items()
            if k.startswith("golem.com.payment.platform") and v
        ]

        # Update wallets list
        wallets_list.extend(wallets)

        # Update providers dictionary
        if provider_id and provider_name:
            providers_dict[(provider_id, provider_name)].append(offer.provider.node_id)

    # Deduplicate wallets
    wallets_list = list(set(wallets_list))

    # Convert providers to list of dictionaries
    providers_list = [
        {"provider_name": name, "id": _id}
        for (_id, name), nodes in providers_dict.items()
    ]
    data = {"wallets": wallets_list, "providers": providers_list}
    r.set("wallets_and_ids", json.dumps(data))
