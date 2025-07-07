from django.db import transaction
import urllib.parse
from django.utils.timezone import now
from django.db.models import Sum
from api2.models import GolemTransactions
from collections import defaultdict
from django.db.models.functions import TruncHour, TruncDay
import requests
from django.db.models import DateField
from django.db.models.functions import TruncDay
from core.celery import app
from celery import Celery
import json
import subprocess
import os
import statistics
from api.utils import get_stats_data, get_stats_data_v2
import time
import redis
from django.conf import settings
from datetime import datetime, timedelta, date
from .models import (
    Node,
    NetworkStats,
    NetworkStatsMax,
    ProvidersComputing,
    NetworkAveragePricing,
    NetworkMedianPricing,
    NetworkAveragePricingMax,
    NetworkMedianPricingMax,
    ProvidersComputingMax,
    Network,
    Requestors,
    requestor_scraper_check,
)

from api2.models import Node as Nodev2, Offer
from django.db.models import Max, Avg, Min
from api.models import APIHits
from api.serializers import (
    NodeSerializer,
    NetworkMedianPricingMaxSerializer,
    NetworkAveragePricingMaxSerializer,
    ProvidersComputingMaxSerializer,
    NetworkStatsMaxSerializer,
    NetworkStatsSerializer,
    RequestorSerializer,
)
from django.utils import timezone
import logging

# jsonmsg = {"user_id": elem, "path": "/src/data/user_avatars/" + elem + ".png"}
# r.lpush("image_classifier", json.dumps(jsonmsg))

pool = redis.ConnectionPool(host="redis", port=6379, db=0)
r = redis.Redis(connection_pool=pool)


@app.task
def save_endpoint_logs_to_db():
    length = r.llen("API")
    # Remove entries in list
    r.delete("API")
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
    jsondata = {"count": obj.count}
    serialized = json.dumps(jsondata)
    r.set("api_requests", serialized)


@app.task
def requestors_to_redis():
    query = Requestors.objects.all().order_by("-tasks_requested")
    serializer = RequestorSerializer(query, many=True)
    data = json.dumps(serializer.data)
    r.set("requestors", data)


@app.task
def stats_snapshot_yesterday():
    start_date = date.today() - timedelta(days=1)
    date_trunc_day = TruncDay("date", output_field=DateField())

    online = (
        NetworkStats.objects.filter(date__gte=start_date, runtime="vm")
        .annotate(day=date_trunc_day)
        .values("day")
        .annotate(online=Max("online"))
    )
    cores = (
        NetworkStats.objects.filter(date__gte=start_date, runtime="vm")
        .annotate(day=date_trunc_day)
        .values("day")
        .annotate(cores=Max("cores"))
    )
    memory = (
        NetworkStats.objects.filter(date__gte=start_date, runtime="vm")
        .annotate(day=date_trunc_day)
        .values("day")
        .annotate(memory=Max("memory"))
    )
    disk = (
        NetworkStats.objects.filter(date__gte=start_date, runtime="vm")
        .annotate(day=date_trunc_day)
        .values("day")
        .annotate(disk=Max("disk"))
    )

    existing_dates = NetworkStatsMax.objects.filter(runtime="vm").values_list(
        "date", flat=True
    )

    for online_obj, cores_obj, memory_obj, disk_obj in zip(online, cores, memory, disk):
        current_date = online_obj["day"]
        if current_date not in existing_dates:
            NetworkStatsMax.objects.create(
                online=online_obj["online"],
                cores=cores_obj["cores"],
                memory=memory_obj["memory"],
                disk=disk_obj["disk"],
                date=current_date,
            )


@app.task
def computing_snapshot_yesterday():
    start_date = date.today() - timedelta(days=1)
    date_trunc_day = TruncDay("date", output_field=DateField())

    computing = (
        ProvidersComputing.objects.filter(date__gte=start_date)
        .annotate(day=date_trunc_day)
        .values("day")
        .annotate(total=Max("total"))
    )

    existing_dates = ProvidersComputingMax.objects.all().values_list("date", flat=True)

    for obj in computing:
        if obj["day"] not in existing_dates:
            ProvidersComputingMax.objects.create(
                total=obj["total"], date=obj["day"])


@app.task
def pricing_snapshot_yesterday():
    start_date = date.today() - timedelta(days=1)
    date_trunc_day = TruncDay("date", output_field=DateField())

    avg_prices = (
        NetworkAveragePricing.objects.filter(date__gte=start_date)
        .annotate(day=date_trunc_day)
        .values("day")
        .annotate(start=Avg("start"), cpuh=Avg("cpuh"), perh=Avg("perh"))
    )
    median_prices = (
        NetworkMedianPricing.objects.filter(date__gte=start_date)
        .annotate(day=date_trunc_day)
        .values("day")
        .annotate(start=Min("start"), cpuh=Min("cpuh"), perh=Min("perh"))
    )

    existing_avg_dates = NetworkAveragePricingMax.objects.all().values_list(
        "date", flat=True
    )
    existing_median_dates = NetworkMedianPricingMax.objects.all().values_list(
        "date", flat=True
    )

    for avg_obj, median_obj in zip(avg_prices, median_prices):
        if avg_obj["day"] not in existing_avg_dates:
            NetworkAveragePricingMax.objects.create(
                start=avg_obj["start"],
                cpuh=avg_obj["cpuh"],
                perh=avg_obj["perh"],
                date=avg_obj["day"],
            )
        if median_obj["day"] not in existing_median_dates:
            NetworkMedianPricingMax.objects.create(
                start=median_obj["start"],
                cpuh=median_obj["cpuh"],
                perh=median_obj["perh"],
                date=median_obj["day"],
            )


@app.task
def network_average_pricing():
    perhour = []
    cpuhour = []
    start = []
    data = Node.objects.filter(online=True)
    for obj in data:
        if (
            str(obj.data["golem.runtime.name"]) == "vm"
            or str(obj.data["golem.runtime.name"]) == "wasmtime"
        ):
            pricing_vector = {
                obj.data["golem.com.usage.vector"][0]: obj.data[
                    "golem.com.pricing.model.linear.coeffs"
                ][0],
                obj.data["golem.com.usage.vector"][1]: obj.data[
                    "golem.com.pricing.model.linear.coeffs"
                ][1],
            }
            if len(str(pricing_vector["golem.usage.duration_sec"])) < 5:
                perhour.append(pricing_vector["golem.usage.duration_sec"])
            else:
                perhour.append(
                    pricing_vector["golem.usage.duration_sec"] * 3600)

            start.append(obj.data["golem.com.pricing.model.linear.coeffs"][2])
            if len(str(pricing_vector["golem.usage.cpu_sec"])) < 5:
                cpuhour.append(pricing_vector["golem.usage.cpu_sec"])
            else:
                cpuhour.append(pricing_vector["golem.usage.cpu_sec"] * 3600)

    # Check if any of the lists are empty
    if not (perhour and cpuhour and start):
        # Skip if any list is empty
        return

    # Calculate mean if all lists have data
    cpuhour_avg = statistics.mean(cpuhour)
    perhour_avg = statistics.mean(perhour)
    start_avg = statistics.mean(start)

    content = {
        "cpuhour": cpuhour_avg,
        "perhour": perhour_avg,
        "start": start_avg,
    }
    serialized = json.dumps(content)
    NetworkAveragePricing.objects.create(
        start=start_avg,
        cpuh=cpuhour_avg,
        perh=perhour_avg,
    )
    r.set("network_average_pricing", serialized)


@app.task
def network_median_pricing():
    perhour = []
    cpuhour = []
    startprice = []
    data = Node.objects.filter(online=True)
    for obj in data:
        if (
            str(obj.data["golem.runtime.name"]) == "vm"
            or str(obj.data["golem.runtime.name"]) == "wasmtime"
        ):
            pricing_vector = {
                obj.data["golem.com.usage.vector"][0]: obj.data[
                    "golem.com.pricing.model.linear.coeffs"
                ][0],
                obj.data["golem.com.usage.vector"][1]: obj.data[
                    "golem.com.pricing.model.linear.coeffs"
                ][1],
            }
            if len(str(pricing_vector["golem.usage.duration_sec"])) < 5:
                perhour.append(pricing_vector["golem.usage.duration_sec"])
            else:
                perhour.append(
                    pricing_vector["golem.usage.duration_sec"] * 3600)

                startprice.append(
                    (obj.data["golem.com.pricing.model.linear.coeffs"][2])
                )
            if len(str(pricing_vector["golem.usage.cpu_sec"])) < 5:
                cpuhour.append(pricing_vector["golem.usage.cpu_sec"])
            else:
                cpuhour.append(pricing_vector["golem.usage.cpu_sec"] * 3600)

    if not perhour:
        return
    if not cpuhour:
        return
    if not startprice:
        return

    content = {
        "cpuhour": statistics.median(cpuhour),
        "perhour": statistics.median(perhour),
        "start": statistics.median(startprice),
    }
    serialized = json.dumps(content)
    NetworkMedianPricing.objects.create(
        start=statistics.median(startprice),
        cpuh=statistics.median(cpuhour),
        perh=statistics.median(perhour),
    )
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

    data4 = NetworkStatsMax.objects.filter(runtime="vm")
    serializerstats = NetworkStatsMaxSerializer(data4, many=True)
    statsmax = json.dumps(serializerstats.data)
    r.set("stats_max", statsmax)


@app.task
def network_stats_to_redis():
    cores = []
    threads = []
    memory = []
    disk = []

    # Filter Offers with runtime 'vm' and related online Nodes
    vm_offers_query = Offer.objects.filter(
        runtime="vm",
        provider__online=True,  # Accessing related Node instances that are online
    )

    for offer in vm_offers_query:
        properties = offer.properties

        if properties:
            # Extracting the required properties from the JSON field
            cores.append(properties.get("golem.inf.cpu.cores", 0))
            threads.append(properties.get("golem.inf.cpu.threads", 0))
            memory.append(properties.get("golem.inf.mem.gib", 0.0))
            disk.append(properties.get("golem.inf.storage.gib", 0.0))

    # Filtering for mainnet and testnet
    mainnet_offers = vm_offers_query.filter(
        properties__has_key="golem.com.payment.platform.erc20-mainnet-glm.address"
    )
    testnet_offers = vm_offers_query.exclude(
        properties__has_key="golem.com.payment.platform.erc20-mainnet-glm.address"
    )

    content = {
        "online": vm_offers_query.count(),
        "cores": sum(cores),
        "threads": sum(threads),
        "memory": sum(memory),
        "disk": sum(disk),
        "mainnet": mainnet_offers.count(),
        "testnet": testnet_offers.count(),
    }

    # Further analysis and serialization
    # (The mainnet and testnet logic might need to be revised based on how they relate to the Offer model)

    serialized = json.dumps(content)

    r.set("online_stats", serialized)


@app.task
def networkstats_30m():
    now = datetime.now()
    before = now - timedelta(minutes=30)
    data = NetworkStats.objects.filter(
        date__range=(before, now), runtime="vm"
    ).order_by("date")
    serializer = NetworkStatsSerializer(data, many=True)
    r.set("stats_30m", json.dumps(serializer.data))


@app.task
def network_utilization_to_redis():
    end = round(time.time())
    start = end - 21600
    domain = (
        os.environ.get("STATS_URL")
        + f"api/datasources/uid/dec5owmc8gt8ge/resources/api/v1/query_range?query=sum(market_agreements_provider_approved%7Bexported_job%3D~%22{settings.GRAFANA_JOB_NAME}%22%7D%20-%20market_agreements_provider_terminated%7Bexported_job%3D~%22{settings.GRAFANA_JOB_NAME}%22%7D)&start={start}&end={end}&step=30"
    )
    content = get_stats_data(domain)
    if content[1] == 200:
        serialized = json.dumps(content[0])
        r.set("network_utilization", serialized)


@app.task
def network_node_versions():
    from django.db import transaction

    now = round(time.time())
    domain = (
        os.environ.get("STATS_URL")
        + f'api/datasources/uid/dec5owmc8gt8ge/resources/api/v1/query?query=yagna_version_major%7Bexported_job%3D"{settings.GRAFANA_JOB_NAME}"%7D*100%2Byagna_version_minor%7Bexported_job%3D"{settings.GRAFANA_JOB_NAME}"%7D*10%2Byagna_version_patch%7Bexported_job%3D"{settings.GRAFANA_JOB_NAME}"%7D&time={now}'
    )
    data = get_stats_data(domain)
    nodes_data = data[0]["data"]["result"]

    node_updates = []

    for obj in nodes_data:
        try:
            node_id = obj["metric"]["exported_instance"]
            version_val = int(obj["value"][1])

            if len(str(version_val)) == 2:  # Two-digit version
                major = 0
                minor = version_val // 10
                patch = version_val % 10
            elif len(str(version_val)) == 3:  # Three-digit version
                major = 0
                minor = version_val // 10
                patch = version_val % 10
            else:
                continue  # Skip if not two or three digits

            version_formatted = f"{major}.{minor}.{patch}"
            node_updates.append((node_id, version_formatted))
        except Exception as e:
            print(f"Error processing node version: {e}")

    for node_id, version in node_updates:
        Node.objects.filter(node_id=node_id).update(version=version)
        Nodev2.objects.filter(node_id=node_id).update(version=version)


@app.task
def network_versions_to_redis():
    now = round(time.time())
    domain = (
        os.environ.get("STATS_URL")
        + f'api/datasources/uid/dec5owmc8gt8ge/resources/api/v1/query_range?query=count_values("version"%2C%20yagna_version_major%7Bexported_job%3D"{settings.GRAFANA_JOB_NAME}"%7D*100%2Byagna_version_minor%7Bexported_job%3D"{settings.GRAFANA_JOB_NAME}"%7D*10%2Byagna_version_patch%7Bexported_job%3D"{settings.GRAFANA_JOB_NAME}"%7D)&start={now}&end={now}&step=5'
    )
    content = get_stats_data(domain)
    print(content)
    if content[1] == 200:
        versions_nonsorted = []
        versions = []
        data = content[0]["data"]["result"]
        for obj in data:
            versions_nonsorted.append(
                {"version": int(obj["metric"]["version"]),
                 "count": obj["values"][0][1]}
            )

        versions_nonsorted.sort(key=lambda x: x["version"], reverse=False)

        # Function to fetch all pages of releases
        def fetch_all_releases(url, access_token):
            headers = {"Authorization": f"token {access_token}"}
            releases = []
            while url:
                response = requests.get(url, headers=headers)
                page_releases = response.json()
                if page_releases:
                    releases.extend(page_releases)
                    if "Link" in response.headers:
                        links = response.headers["Link"].split(", ")
                        next_link = [
                            link for link in links if 'rel="next"' in link]
                        if next_link:
                            url = next_link[0][
                                next_link[0].find("<") + 1: next_link[0].find(">")
                            ]
                        else:
                            url = None
                    else:
                        url = None
                else:
                    break
            return releases

        releases_data = fetch_all_releases(
            "https://api.github.com/repos/golemfactory/yagna/releases",
            os.environ.get("GITHUB_AUTH_TOKEN_NON_PRIVILEDGED"),
        )

        def find_release_by_tag(tag, releases_data):
            # Check for an exact match first
            for release in releases_data:
                if release["tag_name"].lower() == tag.lower():
                    return "official"

            # If no exact match is found, check for partial matches where "prerelease": true
            for release in releases_data:
                if (
                    tag.lower() in release["tag_name"].lower()
                    and release["prerelease"] == True
                ):
                    return "rc"
            return "rc"

        versions_nonsorted.sort(key=lambda x: x["version"], reverse=False)

        # Calculate the total count for all versions
        total_count = sum(int(obj["count"]) for obj in versions_nonsorted)

        # Function definitions for fetch_all_releases and find_release_by_tag remain the same...

        for obj in versions_nonsorted:
            version = str(obj["version"])
            count = obj["count"]
            if len(version) == 2:
                concatinated = "0." + version[0] + "." + version[1]
            elif len(version) == 3:
                concatinated = "0." + version[0] + \
                    version[1] + "." + version[2]

            tag_to_search = "v" + concatinated
            release_status = find_release_by_tag(tag_to_search, releases_data)

            # Calculate the percentage of the total for this version
            percentage_of_total = round((int(count) / total_count) * 100, 2)

            if release_status == "official":
                versions.append(
                    {
                        "version": concatinated,
                        "count": count,
                        "rc": False,
                        "percentage": percentage_of_total,
                    }
                )
            elif release_status == "rc":
                versions.append(
                    {
                        "version": "RC-" + concatinated,
                        "count": count,
                        "rc": True,
                        "percentage": percentage_of_total,
                    }
                )

        serialized = json.dumps(versions)
        r.set("network_versions", serialized)


def get_earnings(platform, hours):
    end = round(time.time())
    domain = (
        os.environ.get("STATS_URL") +
        f"api/datasources/uid/dec5owmc8gt8ge/resources/api/v1/query?query="
        f'sum(increase(payment_amount_received%7Bexported_job%3D~"{settings.GRAFANA_JOB_NAME}"%2C%20platform%3D"{platform}"%7D%5B{hours}%5D)%2F10%5E9)&time={end}'
    )
    data = get_stats_data(domain)
    if data[1] == 200 and data[0]["data"]["result"]:
        return round(float(data[0]["data"]["result"][0]["value"][1]), 2)
    return 0.0


@app.task
def network_earnings(hours):
    # Platforms to check
    platforms = settings.GOLEM_MAINNET_PAYMENT_DRIVERS

    # Calculating earnings for each platform
    total_earnings = sum(get_earnings(platform, hours)
                         for platform in platforms)

    content = {"total_earnings": round(total_earnings, 2)}
    serialized = json.dumps(content)

    # Assuming 'r' is a Redis connection
    r = redis.Redis(host="redis", port=6379, db=0)
    r.set(f"network_earnings_{hours}", serialized)


@app.task
def fetch_yagna_release():
    url = "https://api.github.com/repos/golemfactory/yagna/releases"
    headers = {"Accept": "application/vnd.github.v3+json"}
    releases_info = []

    while url:
        response = requests.get(url, headers=headers)
        releases = response.json()
        for release in releases:
            if not release["prerelease"]:
                release_data = {
                    "tag_name": release["tag_name"],
                    "published_at": release["published_at"],
                }
                releases_info.append(release_data)
        if "next" in response.links:
            url = response.links["next"]["url"]
        else:
            url = None

    serialized = json.dumps(releases_info)
    r.set("yagna_releases", serialized)


@app.task
def network_total_earnings():
    end = round(time.time())
    network_types = settings.GOLEM_MAINNET_PAYMENT_DRIVERS

    for network in network_types:
        domain = (
            os.environ.get("STATS_URL")
            + f'api/datasources/uid/dec5owmc8gt8ge/resources/api/v1/query?query=sum(increase(payment_amount_received%7Bexported_job%3D~"{settings.GRAFANA_JOB_NAME}"%2C%20platform%3D"{network}"%7D%5B2m%5D)%2F10%5E9)&time={end}'
        )
        update_total_earnings(domain)


@app.task
def network_earnings_overview_new():
    time_frames = [6, 24, 168, 720, 2160]
    response_data = {}

    for frame in time_frames:
        end_time = now()
        start_time = end_time - timedelta(hours=frame)
        total_earnings = (
            GolemTransactions.objects.filter(
                timestamp__range=[start_time, end_time], tx_from_golem=True
            ).aggregate(Sum("amount"))["amount__sum"]
            or 0.0
        )

        response_data[f"network_earnings_{frame}h"] = {
            "total_earnings": float(total_earnings)
        }

    all_time_earnings = (
        GolemTransactions.objects.filter(tx_from_golem=True).aggregate(Sum("amount"))[
            "amount__sum"
        ]
        or 0.0
    )

    response_data["network_total_earnings"] = {
        "total_earnings": float(all_time_earnings)
    }

    r.set("network_earnings_overview_new", json.dumps(response_data))


def update_total_earnings(domain):
    data = get_stats_data(domain)
    if data[1] == 200 and data[0]["data"]["result"]:
        network_value = float(data[0]["data"]["result"][0]["value"][1])
        if network_value > 0:
            db, created = Network.objects.get_or_create(id=1)
            db.total_earnings = (
                network_value
                if created or db.total_earnings is None
                else db.total_earnings + network_value
            )
            db.save()
            content = {"total_earnings": db.total_earnings}
            serialized = json.dumps(content)
            r.set("network_total_earnings", serialized)


@app.task
def computing_now_to_redis():
    end = round(time.time())
    start = round(time.time()) - int(10)
    domain = (
        os.environ.get("STATS_URL")
        + f"api/datasources/uid/dec5owmc8gt8ge/resources/api/v1/query_range?query=sum(activity_provider_created%7Bexported_job%3D~%22{settings.GRAFANA_JOB_NAME}%22%7D%20-%20activity_provider_destroyed%7Bexported_job%3D~%22{settings.GRAFANA_JOB_NAME}%22%7D)&start={start}&end={end}&step=1"
    )
    data = get_stats_data(domain)
    if data[1] == 200:
        if data[0]["data"]["result"]:
            content = {"computing_now": data[0]
                       ["data"]["result"][0]["values"][-1][1]}
            ProvidersComputing.objects.create(
                total=data[0]["data"]["result"][0]["values"][-1][1]
            )
            serialized = json.dumps(content)
            r.set("computing_now", serialized)


@app.task
def providers_average_earnings_to_redis():
    platforms = settings.GOLEM_MAINNET_PAYMENT_DRIVERS

    end = round(time.time())
    total_average_earnings = 0.0

    for platform in platforms:
        domain = (
            os.environ.get("STATS_URL")
            + f'api/datasources/uid/dec5owmc8gt8ge/resources/api/v1/query?query=avg(increase(payment_amount_received%7Bexported_job%3D~"{settings.GRAFANA_JOB_NAME}"%2C%20platform%3D"{platform}"%7D%5B24h%5D)%2F10%5E9)&time={end}'
        )
        data = get_stats_data(domain)
        if data[1] == 200 and data[0]["data"]["result"]:
            platform_average = round(
                float(data[0]["data"]["result"][0]["value"][1]), 4)
        else:
            platform_average = 0.0
        total_average_earnings += platform_average

    content = {"average_earnings": total_average_earnings}
    serialized = json.dumps(content)
    r.set("provider_average_earnings", serialized)


@app.task
def paid_invoices_1h():
    end = round(time.time())
    domain = (
        os.environ.get("STATS_URL")
        + f'api/datasources/uid/dec5owmc8gt8ge/resources/api/v1/query?query=sum(increase(payment_invoices_provider_paid%7Bexported_job%3D~"{settings.GRAFANA_JOB_NAME}"%7D%5B1h%5D))%2Fsum(increase(payment_invoices_provider_sent%7Bexported_job%3D~"{settings.GRAFANA_JOB_NAME}"%7D%5B1h%5D))&time={end}'
    )
    data = get_stats_data(domain)
    if data[1] == 200:
        if data[0]["data"]["result"]:
            content = {
                "percentage_paid": float(data[0]["data"]["result"][0]["value"][1]) * 100
            }
            serialized = json.dumps(content)
            r.set("paid_invoices_1h", serialized)


@app.task
def provider_accepted_invoices_1h():
    end = round(time.time())
    domain = (
        os.environ.get("STATS_URL")
        + f'api/datasources/uid/dec5owmc8gt8ge/resources/api/v1/query?query=sum(increase(payment_invoices_provider_accepted%7Bexported_job%3D~"{settings.GRAFANA_JOB_NAME}"%7D%5B1h%5D))%2Fsum(increase(payment_invoices_provider_sent%7Bexported_job%3D~"{settings.GRAFANA_JOB_NAME}"%7D%5B1h%5D))&time={end}'
    )
    data = get_stats_data(domain)
    if data[1] == 200:
        if data[0]["data"]["result"]:
            content = {
                "percentage_invoice_accepted": float(
                    data[0]["data"]["result"][0]["value"][1]
                )
                * 100
            }
            serialized = json.dumps(content)
            r.set("provider_accepted_invoice_percentage", serialized)


def get_earnings_for_node_on_platform(user_node_id, platform):
    now = round(time.time())
    domain = (
        os.environ.get("STATS_URL")
        + f'api/datasources/uid/dec5owmc8gt8ge/resources/api/v1/query?query=sum(increase(payment_amount_received%7Bhostname%3D~"{user_node_id}"%2C%20platform%3D"{platform}"%7D%5B10m%5D)%2F10%5E9)&time={now}'
    )
    data = get_stats_data(domain)
    try:
        if data[0]["data"]["result"]:
            return round(float(data[0]["data"]["result"][0]["value"][1]), 2)
        else:
            return 0.0
    except Exception as e:
        print(f"Error getting data for {user_node_id} on {platform}", e)
        return 0.0


@app.task
def node_earnings_total(node_version):
    if node_version == "v1":
        providers = Node.objects.filter(
            online=True).only("node_id", "earnings_total")
    elif node_version == "v2":
        providers = Nodev2.objects.filter(
            online=True).only("node_id", "earnings_total")

    providers_updates = []
    for provider in providers:
        earnings_total = sum(
            get_earnings_for_node_on_platform(provider.node_id, platform)
            for platform in settings.GOLEM_MAINNET_PAYMENT_DRIVERS
        )
        updated_earnings_total = (
            provider.earnings_total + earnings_total
            if provider.earnings_total
            else earnings_total
        )
        providers_updates.append((provider.pk, updated_earnings_total))

    if node_version == "v1":
        Node.objects.bulk_update(
            [
                Node(pk=pk, earnings_total=earnings)
                for pk, earnings in providers_updates
            ],
            ["earnings_total"],
        )
    elif node_version == "v2":
        Nodev2.objects.bulk_update(
            [
                Nodev2(pk=pk, earnings_total=earnings)
                for pk, earnings in providers_updates
            ],
            ["earnings_total"],
        )


@app.task
def market_agreement_termination_reasons():
    end = round(time.time())
    start = round(time.time()) - int(10)
    content = {}
    domain_success = (
        os.environ.get("STATS_URL")
        + f'api/datasources/uid/dec5owmc8gt8ge/resources/api/v1/query?query=sum(increase(market_agreements_provider_terminated_reason%7Bexported_job%3D"{settings.GRAFANA_JOB_NAME}"%2C%20reason%3D"Success"%7D%5B1h%5D))&time={end}'
    )
    data_success = get_stats_data(domain_success)
    if data_success[1] == 200:
        if data_success[0]["data"]["result"]:
            content["market_agreements_success"] = round(
                float(data_success[0]["data"]["result"][0]["value"][1])
            )
    # Failure
    domain_cancelled = (
        os.environ.get("STATS_URL")
        + f'api/datasources/uid/dec5owmc8gt8ge/resources/api/v1/query?query=sum(increase(market_agreements_provider_terminated_reason%7Bexported_job%3D"{settings.GRAFANA_JOB_NAME}"%2C%20reason%3D"Cancelled"%7D%5B6h%5D))&time={end}'
    )
    data_cancelled = get_stats_data(domain_cancelled)
    if data_cancelled[1] == 200:
        if data_cancelled[0]["data"]["result"]:
            content["market_agreements_cancelled"] = round(
                float(data_cancelled[0]["data"]["result"][0]["value"][1])
            )
    # Expired
    domain_expired = (
        os.environ.get("STATS_URL")
        + f'api/datasources/uid/dec5owmc8gt8ge/resources/api/v1/query?query=sum(increase(market_agreements_provider_terminated_reason%7Bexported_job%3D"{settings.GRAFANA_JOB_NAME}"%2C%20reason%3D"Expired"%7D%5B6h%5D))&time={end}'
    )
    data_expired = get_stats_data(domain_expired)
    if data_expired[1] == 200:
        if data_expired[0]["data"]["result"]:
            content["market_agreements_expired"] = round(
                float(data_expired[0]["data"]["result"][0]["value"][1])
            )
    # RequestorUnreachable
    domain_unreachable = (
        os.environ.get("STATS_URL")
        + f'api/datasources/uid/dec5owmc8gt8ge/resources/api/v1/query?query=sum(increase(market_agreements_provider_terminated_reason%7Bexported_job%3D"{settings.GRAFANA_JOB_NAME}"%2C%20reason%3D"RequestorUnreachable"%7D%5B6h%5D))&time={end}'
    )
    data_unreachable = get_stats_data(domain_unreachable)
    if data_unreachable[1] == 200:
        if data_unreachable[0]["data"]["result"]:
            content["market_agreements_requestorUnreachable"] = round(
                float(data_unreachable[0]["data"]["result"][0]["value"][1])
            )

    # DebitNotesDeadline
    domain_debitdeadline = (
        os.environ.get("STATS_URL")
        + f'api/datasources/uid/dec5owmc8gt8ge/resources/api/v1/query?query=sum(increase(market_agreements_provider_terminated_reason%7Bexported_job%3D"{settings.GRAFANA_JOB_NAME}"%2C%20reason%3D"DebitNotesDeadline"%7D%5B6h%5D))&time={end}'
    )
    data_debitdeadline = get_stats_data(domain_debitdeadline)
    if data_debitdeadline[1] == 200:
        if data_debitdeadline[0]["data"]["result"]:
            content["market_agreements_debitnoteDeadline"] = round(
                float(data_debitdeadline[0]["data"]["result"][0]["value"][1])
            )
    serialized = json.dumps(content)
    r.set("market_agreement_termination_reasons", serialized)


@app.task
def requestor_scraper():
    checker, checkcreated = requestor_scraper_check.objects.get_or_create(id=1)
    update_frequency = 10

    if checkcreated:
        checker.indexed_before = True
        checker.save()
        now = round(time.time())
        ninetydaysago = now - 7776000
        update_frequency = 3600
        time_to_check = ninetydaysago
    else:
        time_to_check = round(time.time() - update_frequency)

    query = f'increase(market_agreements_requestor_approved{{exported_job="{settings.GRAFANA_JOB_NAME}"}}[{update_frequency}s]) > 0'

    data = get_stats_data_v2(
        query=query,
        time_from=time_to_check,
        time_to=time_to_check
    )

    if data[1] == 200 and data[0].get('results', {}).get('A', {}).get('frames'):
        frames = data[0]['results']['A']['frames']

        for frame in frames:
            if 'data' not in frame or 'values' not in frame['data']:
                continue

            # Get the requestor address from the fields
            requestor_address = None
            for field in frame['schema']['fields']:
                if field.get('type') == 'number':
                    labels = field.get('labels', {})
                    requestor_address = labels.get('exported_instance')
                    if requestor_address:
                        break

            if not requestor_address:
                continue

            # Get the value from the data
            values = frame['data']['values']
            if len(values) >= 2 and len(values[1]) > 0:
                tasks_count = float(values[1][0])

                try:
                    # Create or update the Requestors object
                    obj, created = Requestors.objects.get_or_create(
                        node_id=requestor_address,
                        defaults={'tasks_requested': tasks_count}
                    )
                    
                    if not created:
                        obj.tasks_requested = (obj.tasks_requested or 0) + tasks_count
                        obj.save()

                except Exception as e:
                    print(f"Error saving requestor data: {e}")


