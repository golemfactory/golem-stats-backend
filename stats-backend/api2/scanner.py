#!/usr/bin/env python3
import asyncio
import csv
import json
import redis
import pathlib
import sys
import subprocess
import requests
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
from .utils import identify_network_by_offer, identify_wallet_and_network
from django.db import transaction
from django.db.models import OuterRef, Subquery

from concurrent.futures import ThreadPoolExecutor

pool = redis.ConnectionPool(host="redis", port=6379, db=0)
r = redis.Redis(connection_pool=pool)


@app.task
def update_providers_info(node_props):
    now = timezone.now()
    days_in_current_month = calendar.monthrange(now.year, now.month)[1]
    seconds_current_month = days_in_current_month * 24 * 60 * 60
    hours_in_current_month = days_in_current_month * 24
    glm_usd_value = GLM.objects.get(id=1)

    # Collect provider_ids and data
    provider_data_list = []
    for prop in node_props:
        data = json.loads(prop)
        provider_id = data["node_id"]
        provider_data_list.append((provider_id, data))

    provider_ids = [provider_id for provider_id, _ in provider_data_list]

    # Get existing Nodes
    existing_nodes = Node.objects.filter(node_id__in=provider_ids)
    existing_nodes_dict = {node.node_id: node for node in existing_nodes}

    # Find which nodes are new
    existing_provider_ids = set(existing_nodes_dict.keys())
    new_provider_ids = set(provider_ids) - existing_provider_ids

    # Create new Node instances if any
    new_nodes = [Node(node_id=provider_id) for provider_id in new_provider_ids]
    if new_nodes:
        Node.objects.bulk_create(new_nodes)

    # Update existing_nodes_dict with newly created nodes
    updated_nodes = Node.objects.filter(node_id__in=new_provider_ids)
    for node in updated_nodes:
        existing_nodes_dict[node.node_id] = node

    # Now process offers
    offer_keys = []
    offer_data = {}  # key: (provider_id, runtime), value: data
    for provider_id, data in provider_data_list:
        runtime = data["golem.runtime.name"]
        offer_key = (provider_id, runtime)
        offer_keys.append(offer_key)
        offer_data[offer_key] = data

    # Get existing Offers
    existing_offers = Offer.objects.filter(
        provider__node_id__in=provider_ids,
        runtime__in=[data["golem.runtime.name"] for _, data in provider_data_list]
    ).select_related('provider')

    existing_offers_dict = {(offer.provider.node_id, offer.runtime): offer for offer in existing_offers}

    # Find which offers are new
    existing_offer_keys = set(existing_offers_dict.keys())
    new_offer_keys = set(offer_keys) - existing_offer_keys

    # Create new Offer instances if any
    new_offers = []
    for offer_key in new_offer_keys:
        provider_id, runtime = offer_key
        provider = existing_nodes_dict[provider_id]
        new_offers.append(Offer(provider=provider, runtime=runtime))

    if new_offers:
        Offer.objects.bulk_create(new_offers)

    # Update existing_offers_dict with newly created offers
    updated_offers = Offer.objects.filter(provider__node_id__in=provider_ids).select_related('provider')
    existing_offers_dict.update({(offer.provider.node_id, offer.runtime): offer for offer in updated_offers})

    # Now process and update offers
    offers_to_update = []  # list of offers to bulk update
    for offer_key in offer_keys:
        provider_id, runtime = offer_key
        data = offer_data[offer_key]
        offer = existing_offers_dict.get(offer_key)
        if not offer:
            continue

        vectors = {}
        if data["golem.runtime.name"] in ("vm", "vm-nvidia"):
            for key, value in enumerate(data["golem.com.usage.vector"]):
                vectors[value] = key
            MAX_PRICE_CAP_VALUE = 9999999
            monthly_pricing = (
                data["golem.com.pricing.model.linear.coeffs"][vectors["golem.usage.duration_sec"]] * seconds_current_month +
                data["golem.com.pricing.model.linear.coeffs"][vectors["golem.usage.cpu_sec"]] * seconds_current_month * data["golem.inf.cpu.threads"] +
                data["golem.com.pricing.model.linear.coeffs"][-1]
            )
            if not monthly_pricing:
                print(f"Monthly price is {monthly_pricing}")
            offer.monthly_price_glm = min(monthly_pricing, MAX_PRICE_CAP_VALUE)
            offer.monthly_price_usd = min(monthly_pricing * glm_usd_value.current_price, MAX_PRICE_CAP_VALUE)
            offer.hourly_price_glm = min(monthly_pricing / hours_in_current_month, MAX_PRICE_CAP_VALUE)
            offer.hourly_price_usd = min(offer.monthly_price_usd / hours_in_current_month, MAX_PRICE_CAP_VALUE)

            vcpu_needed = data.get("golem.inf.cpu.threads", 0)
            memory_needed = data.get("golem.inf.mem.gib", 0.0)
            # Find closest EC2 instance
            closest_ec2 = EC2Instance.objects.annotate(
                cpu_diff=Abs(F("vcpu") - vcpu_needed),
                memory_diff=Abs(F("memory") - memory_needed),
            ).order_by("cpu_diff", "memory_diff", "price_usd").first()

            if closest_ec2 and monthly_pricing:
                offer_price_usd = min(monthly_pricing * glm_usd_value.current_price, MAX_PRICE_CAP_VALUE)
                ec2_monthly_price = min(closest_ec2.price_usd * 730, MAX_PRICE_CAP_VALUE)
                if ec2_monthly_price != 0:
                    offer_is_more_expensive = offer_price_usd > ec2_monthly_price
                    offer_is_cheaper = offer_price_usd < ec2_monthly_price
                    offer.is_overpriced = offer_is_more_expensive
                    offer.overpriced_compared_to = closest_ec2 if offer_is_more_expensive else None
                    offer.times_more_expensive = offer_price_usd / float(ec2_monthly_price) if offer_is_more_expensive else None
                    offer.suggest_env_per_hour_price = float(closest_ec2.price_usd) / glm_usd_value.current_price
                    offer.cheaper_than = closest_ec2 if offer_is_cheaper else None
                    offer.times_cheaper = float(ec2_monthly_price) / offer_price_usd if offer_is_cheaper else None
                else:
                    print("EC2 monthly price is zero, cannot compare offer prices.")

        # Always update the offer if any properties have changed
        # Compare existing properties with new data
        if offer.properties != data:
            offer.properties = data
            offers_to_update.append(offer)

    # Bulk update offers if any
    if offers_to_update:
        Offer.objects.bulk_update(
            offers_to_update,
            [
                'monthly_price_glm', 'monthly_price_usd', 'hourly_price_glm', 'hourly_price_usd',
                'is_overpriced', 'overpriced_compared_to', 'times_more_expensive',
                'suggest_env_per_hour_price', 'cheaper_than', 'times_cheaper',
                'properties', 'updated_at'
            ]
        )

    # Update Nodes
    nodes_to_update = []
    for provider_id, data in provider_data_list:
        node = existing_nodes_dict[provider_id]
        node.wallet = data.get("wallet")
        node.network = data.get('network', 'mainnet')
        nodes_to_update.append(node)
    if nodes_to_update:
        Node.objects.bulk_update(nodes_to_update, ['wallet', 'network', 'updated_at'])
    print(f"Done updating {len(provider_ids)} providers")



examples_dir = pathlib.Path(__file__).resolve().parent.parent
sys.path.append(str(examples_dir))
from .yapapi_utils import build_parser, print_env_info, format_usage  # noqa: E402




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
                if event.issuer not in current_scan_providers:
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
            timeout=60,  # 60-second timeout for each scan
        )
    except asyncio.TimeoutError:
        print("Scan timeout reached")
    print(f"In the current scan, we found {len(current_scan_providers)} providers")
    # Delay update_nodes_data call using Celery

    update_providers_info.delay(node_props)
