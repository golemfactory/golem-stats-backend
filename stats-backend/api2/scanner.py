#!/usr/bin/env python3
import asyncio
import csv
import json
import redis
import pathlib
import sys
import subprocess
from datetime import datetime, timedelta
from django.utils import timezone
from .models import Node, NodeStatusHistory, GLM, EC2Instance, Offer
from asgiref.sync import sync_to_async
from yapapi import props as yp
from yapapi.config import ApiConfig
from yapapi.log import enable_default_logger
from yapapi.props.builder import DemandBuilder
from yapapi.rest import Configuration, Market
from core.celery import app
from django.db.models import Q
from django.db.models import Case, When, Value, F
from django.db.models.functions import Abs
from django.db import transaction
import calendar
from .utils import identify_network_by_offer,identify_wallet_and_network
from django.db import transaction
from django.db.models import OuterRef, Subquery

pool = redis.ConnectionPool(host="redis", port=6379, db=0)
r = redis.Redis(connection_pool=pool)


@app.task(queue="yagna", options={"queue": "yagna", "routing_key": "yagna"})
def update_providers_info(node_props):
    is_online_checked_providers = set()
    unique_providers = set()  # Initialize a set to track unique providers
    now = timezone.now()
    days_in_current_month = calendar.monthrange(now.year, now.month)[1]
    seconds_current_month = days_in_current_month * 24 * 60 * 60
    hours_in_current_month = days_in_current_month * 24
    glm_usd_value = GLM.objects.get(id=1)
    for prop in node_props:
        data = json.loads(prop)
        provider_id = data["node_id"]
        if "wallet" in data:
            wallet = data["wallet"]
        else:
            wallet = None
        unique_providers.add(provider_id)  # Add provider to the set
        obj, created = Node.objects.get_or_create(node_id=provider_id)
        offerobj, offercreated = Offer.objects.get_or_create(
            provider=obj, runtime=data["golem.runtime.name"]
        )
        vectors = {}
        if (
            data["golem.runtime.name"] == "vm"
            or data["golem.runtime.name"] == "vm-nvidia"
        ):
            for key, value in enumerate(data["golem.com.usage.vector"]):
                vectors[value] = key
            MAX_PRICE_CAP_VALUE = 9999999  # This is an ugly cap to prevent providers from setting an obscure pricing value that results in the math below returning "Infinity" which caused the frontend to error out because its not valid JSON
            monthly_pricing = (
                (
                    data["golem.com.pricing.model.linear.coeffs"][
                        vectors["golem.usage.duration_sec"]
                    ]
                    * seconds_current_month
                )
                + (
                    data["golem.com.pricing.model.linear.coeffs"][
                        vectors["golem.usage.cpu_sec"]
                    ]
                    * seconds_current_month
                    * data["golem.inf.cpu.threads"]
                )
                + data["golem.com.pricing.model.linear.coeffs"][-1]
            )
            if not monthly_pricing:
                print(f"Monthly price is {monthly_pricing}")
            offerobj.monthly_price_glm = min(monthly_pricing, MAX_PRICE_CAP_VALUE)
            offerobj.monthly_price_usd = min(
                monthly_pricing * glm_usd_value.current_price, MAX_PRICE_CAP_VALUE
            )
            offerobj.hourly_price_glm = min(
                monthly_pricing / hours_in_current_month, MAX_PRICE_CAP_VALUE
            )
            offerobj.hourly_price_usd = min(
                (offerobj.monthly_price_usd / hours_in_current_month),
                MAX_PRICE_CAP_VALUE,
            )
            vcpu_needed = data.get("golem.inf.cpu.threads", 0)
            memory_needed = data.get("golem.inf.mem.gib", 0.0)
            closest_ec2 = (
                EC2Instance.objects.annotate(
                    cpu_diff=Abs(F("vcpu") - vcpu_needed),
                    memory_diff=Abs(F("memory") - memory_needed),
                )
                .order_by("cpu_diff", "memory_diff", "price_usd")
                .first()
            )

            if closest_ec2 and monthly_pricing:
                offer_price_usd = min(
                    monthly_pricing * glm_usd_value.current_price, MAX_PRICE_CAP_VALUE
                )
                ec2_monthly_price = min(
                    closest_ec2.price_usd * 730, MAX_PRICE_CAP_VALUE
                )

                # Check if ec2_monthly_price is not zero to avoid ZeroDivisionError
                if ec2_monthly_price != 0:
                    offer_is_more_expensive = offer_price_usd > ec2_monthly_price
                    offer_is_cheaper = offer_price_usd < ec2_monthly_price

                    # Update Offer object fields for expensive comparison
                    offerobj.is_overpriced = offer_is_more_expensive
                    offerobj.overpriced_compared_to = (
                        closest_ec2 if offer_is_more_expensive else None
                    )
                    offerobj.times_more_expensive = (
                        offer_price_usd / float(ec2_monthly_price)
                        if offer_is_more_expensive
                        else None
                    )

                    offerobj.suggest_env_per_hour_price = (
                        float(closest_ec2.price_usd) / glm_usd_value.current_price
                    )

                    # Update Offer object fields for cheaper comparison
                    offerobj.cheaper_than = closest_ec2 if offer_is_cheaper else None
                    offerobj.times_cheaper = (
                        float(ec2_monthly_price) / offer_price_usd
                        if offer_is_cheaper
                        else None
                    )
                else:
                    # Handle the case where ec2_monthly_price is zero
                    # You might want to log this situation or set a default behavior
                    print("EC2 monthly price is zero, cannot compare offer prices.")

        offerobj.properties = data
        offerobj.save()
        obj.runtime = data["golem.runtime.name"]
        obj.wallet = wallet
        if not obj.node_id in is_online_checked_providers:
            # Verify each node's status
            is_online = check_node_status(obj.node_id)
            obj.online = is_online
            is_online_checked_providers.add(obj.node_id)
        obj.network = data['network']

        obj.save()
    print(f"Done updating {len(unique_providers)} providers")




examples_dir = pathlib.Path(__file__).resolve().parent.parent
sys.path.append(str(examples_dir))
from .yapapi_utils import build_parser, print_env_info, format_usage  # noqa: E402

@app.task
def update_nodes_status(nodes_to_update):
    providers_to_update = {}
    for provider_id, is_online_now in nodes_to_update.items():
        provider, created = Node.objects.get_or_create(node_id=provider_id)
        # Get the latest status from Redis
        latest_status = r.get(f"node_status:{provider_id}")

        if latest_status is None:
            print(f"Status not found in Redis for provider {provider_id}")
            # Status not found in Redis, fetch the latest status from the database
            
            latest_status_subquery = (
                NodeStatusHistory.objects.filter(provider=provider)
                .order_by("-timestamp")
                .values("is_online")[:1]
            )

            latest_status_from_db = NodeStatusHistory.objects.filter(
                provider=provider, is_online=Subquery(latest_status_subquery)
            ).first()

            if latest_status_from_db:
                # Compare the latest status from the database with the current status
                if latest_status_from_db.is_online != is_online_now:
                    # Status has changed, update the database and Node.online field
                    NodeStatusHistory.objects.create(
                        provider=provider, is_online=is_online_now
                    )
                    providers_to_update[provider_id] = is_online_now
            else:
                # No previous status found in the database, create a new entry
                NodeStatusHistory.objects.create(
                    provider=provider, is_online=is_online_now
                )
                providers_to_update[provider_id] = is_online_now

            # Store the current status in Redis for future lookups
            r.set(f"node_status:{provider_id}", str(is_online_now))
        else:
            print(f"Status found in Redis for provider {provider_id}")
            # Compare the latest status from Redis with the current status
            if latest_status.decode() != str(is_online_now):
                print(f"Status has changed for provider {provider_id}")
                # Status has changed, update the database and Node.online field
                NodeStatusHistory.objects.create(
                    provider=provider, is_online=is_online_now
                )
                providers_to_update[provider_id] = is_online_now

                # Update the status in Redis
                r.set(f"node_status:{provider_id}", str(is_online_now))
        providers_to_update[provider_id] = is_online_now
    # Bulk update the online status of providers
    for provider_id, is_online in providers_to_update.items():
        Node.objects.filter(node_id=provider_id).update(online=is_online)

from celery import group


@app.task(queue="yagna", options={"queue": "yagna", "routing_key": "yagna"})
def update_nodes_data(node_props):
    is_online_checked_providers = set()
    chunk_size = 100
    nodes_status_to_update = {}
    for props in node_props:
        props = json.loads(props)
        issuer_id = props["node_id"]
        if not issuer_id in is_online_checked_providers:
            is_online_now = check_node_status(issuer_id)
            is_online_checked_providers.add(issuer_id)
            nodes_status_to_update[issuer_id] = is_online_now

    chunks = [
        dict(list(nodes_status_to_update.items())[i : i + chunk_size])
        for i in range(0, len(nodes_status_to_update), chunk_size)
    ]

    # Create a group of Celery tasks to be executed in parallel
    task_group = group(update_nodes_status.s(chunk) for chunk in chunks)

    # Dispatch the group of tasks
    result_group = task_group.apply_async()
    # Deserialize each element in node_props into a dictionary
    deserialized_node_props = [json.loads(props) for props in node_props]

    # Now create the set
    provider_ids_in_props = {props["node_id"] for props in deserialized_node_props}
    latest_online_status = (
        NodeStatusHistory.objects.filter(provider=OuterRef("pk"))
        .order_by("-timestamp")
        .values("is_online")[:1]
    )

    previously_online_providers_ids = (
        Node.objects.annotate(latest_online=Subquery(latest_online_status))
        .filter(latest_online=True)
        .values_list("node_id", flat=True)
    )

    provider_ids_not_in_scan = (
        set(previously_online_providers_ids) - provider_ids_in_props
    )
    nodes_status_to_update_not_in_scan = {}
    for issuer_id in provider_ids_not_in_scan:
        is_online_now = check_node_status(issuer_id)

        nodes_status_to_update_not_in_scan[issuer_id] = is_online_now

    # Create chunks of the dictionary
    chunks = [
        dict(list(nodes_status_to_update_not_in_scan.items())[i : i + chunk_size])
        for i in range(0, len(nodes_status_to_update_not_in_scan), chunk_size)
    ]

    # Create a group of Celery tasks to be executed in parallel
    task_group = group(update_nodes_status.s(chunk) for chunk in chunks)

    # Dispatch the group of tasks
    result_group = task_group.apply_async()
    print(
        f"Finished updating {len(provider_ids_not_in_scan)} providers that we didn't find in the scan, but were online in our previous scan."
    )


def check_node_status(issuer_id):
    try:
        process = subprocess.run(
            ["yagna", "net", "find", issuer_id],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=5,  # 5-second timeout for the subprocess
        )

        # Process finished, return True if it was successful and "seen:" is in the output
        return process.returncode == 0 and "seen:" in process.stdout.decode()
    except subprocess.TimeoutExpired as e:
        print("Timeout reached while checking node status", e)
        return False
    except Exception as e:
        print(f"Unexpected error checking node status: {e}")
        return False


async def list_offers(
    conf: Configuration, subnet_tag: str, current_scan_providers, node_props
):
    async with conf.market() as client:
        market_api = Market(client)
        dbuild = DemandBuilder()
        dbuild.add(yp.NodeInfo(name="some scanning node", subnet_tag=subnet_tag))
        dbuild.add(yp.Activity(expiration=datetime.now(timezone.utc)))

        async with market_api.subscribe(
            dbuild.properties, dbuild.constraints
        ) as subscription:
            async for event in subscription.events():
                data = event.props
                if not event.issuer in current_scan_providers:
                    current_scan_providers.add(event.issuer)
                wallet, network = identify_wallet_and_network(event.props)
                data["wallet"] = wallet
                data["network"] = network
                data["node_id"] = event.issuer
                node_props.append(json.dumps(data))


async def monitor_nodes_status(subnet_tag: str = "public"):
    node_props = []
    current_scan_providers = set()

    # Call list_offers with a timeout
    try:
        await asyncio.wait_for(
            list_offers(
                Configuration(api_config=ApiConfig()),
                subnet_tag=subnet_tag,
                node_props=node_props,
                current_scan_providers=current_scan_providers,
            ),
            timeout=60,  # 30-second timeout for each scan
        )
    except asyncio.TimeoutError:
        print("Scan timeout reached")
    print(f"In the current scan, we found {len(current_scan_providers)} providers")
    # Delay update_nodes_data call using Celery

    update_providers_info.delay(node_props)
    update_nodes_data.delay(node_props)
