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


def normalize_properties(data):
    """Normalize the properties dictionary for consistent comparison."""
    # Sort lists within the dictionary
    for key, value in data.items():
        if isinstance(value, list):
            data[key] = sorted(value)
    return dict(sorted(data.items()))  # Return sorted dictionary


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
    new_nodes = []
    for provider_id in new_provider_ids:
        node, created = Node.objects.get_or_create(
            node_id=provider_id,
            defaults={'type': 'provider'}
        )
        if created:
            new_nodes.append(node)
        existing_nodes_dict[node.node_id] = node

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
        runtime__in=[data["golem.runtime.name"]
                     for _, data in provider_data_list]
    ).select_related('provider')

    existing_offers_dict = {
        (offer.provider.node_id, offer.runtime): offer for offer in existing_offers}

    # Find which offers are new
    existing_offer_keys = set(existing_offers_dict.keys())
    new_offer_keys = set(offer_keys) - existing_offer_keys

    # Create new Offer instances if any
    new_offers = []
    for offer_key in new_offer_keys:
        provider_id, runtime = offer_key
        provider = existing_nodes_dict[provider_id]
        offer, created = Offer.objects.get_or_create(
            provider=provider,
            runtime=runtime,
            defaults={
                'properties': offer_data[offer_key]
            }
        )
        if created:
            new_offers.append(offer)
        existing_offers_dict[(provider_id, runtime)] = offer

    # Update existing_offers_dict with newly created offers
    updated_offers = Offer.objects.filter(
        provider__node_id__in=provider_ids).select_related('provider')
    existing_offers_dict.update(
        {(offer.provider.node_id, offer.runtime): offer for offer in updated_offers})

    # Now process and update offers
    offers_to_update = []  # list of offers to bulk update
    for offer_key in offer_keys:
        provider_id, runtime = offer_key
        data = offer_data[offer_key]
        offer = existing_offers_dict.get(offer_key)
        if not offer:
            continue

        vectors = {}
        if data.get("golem.runtime.name") in ("vm", "vm-nvidia"):
            for key, value in enumerate(data.get("golem.com.usage.vector", [])):
                vectors[value] = key
            MAX_PRICE_CAP_VALUE = 9999999
            monthly_pricing = (
                data["golem.com.pricing.model.linear.coeffs"][vectors["golem.usage.duration_sec"]] * seconds_current_month +
                data["golem.com.pricing.model.linear.coeffs"][vectors["golem.usage.cpu_sec"]] * seconds_current_month * data["golem.inf.cpu.threads"] +
                data["golem.com.pricing.model.linear.coeffs"][-1]
            )

            # Set prices even if they're 0
            new_monthly_price_glm = min(monthly_pricing, MAX_PRICE_CAP_VALUE)
            new_monthly_price_usd = min(
                monthly_pricing * glm_usd_value.current_price, MAX_PRICE_CAP_VALUE)
            new_hourly_price_glm = min(
                monthly_pricing / hours_in_current_month, MAX_PRICE_CAP_VALUE)
            new_hourly_price_usd = min(
                new_monthly_price_usd / hours_in_current_month, MAX_PRICE_CAP_VALUE)

            # Check if any price values have changed
            if (offer.monthly_price_glm != new_monthly_price_glm or
                offer.monthly_price_usd != new_monthly_price_usd or
                offer.hourly_price_glm != new_hourly_price_glm or
                    offer.hourly_price_usd != new_hourly_price_usd):

                offer.monthly_price_glm = new_monthly_price_glm
                offer.monthly_price_usd = new_monthly_price_usd
                offer.hourly_price_glm = new_hourly_price_glm
                offer.hourly_price_usd = new_hourly_price_usd

            vcpu_needed = data.get("golem.inf.cpu.threads", 0)
            memory_needed = data.get("golem.inf.mem.gib", 0.0)
            # Find closest EC2 instance
            closest_ec2 = EC2Instance.objects.annotate(
                cpu_diff=Abs(F("vcpu") - vcpu_needed),
                memory_diff=Abs(F("memory") - memory_needed),
            ).order_by("cpu_diff", "memory_diff", "price_usd").first()

            if closest_ec2 and monthly_pricing:
                offer_price_usd = min(
                    monthly_pricing * glm_usd_value.current_price, MAX_PRICE_CAP_VALUE)
                ec2_monthly_price = min(
                    closest_ec2.price_usd * 730, MAX_PRICE_CAP_VALUE)
                if ec2_monthly_price != 0:
                    offer_is_more_expensive = offer_price_usd > ec2_monthly_price
                    offer_is_cheaper = offer_price_usd < ec2_monthly_price
                    offer.is_overpriced = offer_is_more_expensive
                    offer.overpriced_compared_to = closest_ec2 if offer_is_more_expensive else None
                    offer.times_more_expensive = offer_price_usd / \
                        float(ec2_monthly_price) if offer_is_more_expensive else None
                    offer.suggest_env_per_hour_price = float(
                        closest_ec2.price_usd) / glm_usd_value.current_price
                    offer.cheaper_than = closest_ec2 if offer_is_cheaper else None
                    offer.times_cheaper = float(
                        ec2_monthly_price) / offer_price_usd if offer_is_cheaper else None
                else:
                    print("EC2 monthly price is zero, cannot compare offer prices.")

        # Always update the offer if any properties have changed
        # Compare existing properties with new data
        normalized_existing = normalize_properties(offer.properties.copy())
        normalized_new = normalize_properties(data.copy())

        if normalized_existing != normalized_new:
            print(f"DETECTED CHANGE Updating offer {offer.id}")
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
        # Check if any field has changed before adding to update list
        if (node.wallet != data.get("wallet") or
            node.network != data.get('network', 'mainnet') or
                node.type != "provider"):

            node.wallet = data.get("wallet")
            node.network = data.get('network', 'mainnet')
            node.type = "provider"
            node.updated_at = timezone.now()  # Explicitly set updated_at
            nodes_to_update.append(node)

    if nodes_to_update:
        Node.objects.bulk_update(
            nodes_to_update,
            # Include updated_at in the fields list
            ['wallet', 'network', 'type', 'updated_at']
        )
    print(f"Done updating {len(provider_ids)} providers")

import base64

def flatten_properties(props, prefix=''):
    """
    Recursively flattens a nested dictionary, handling '@tag' patterns as used in golem-base offers.
    If a dict contains '@tag', the value of '@tag' is assigned to the current key, and the rest of the dict
    is flattened under a new prefix using the tag value as the next key.
    """
    flat_props = {}
    for key, value in props.items():
        new_key = f"{prefix}.{key}" if prefix else key
        if isinstance(value, dict):
            # Special handling for golem root
            if key == 'golem':
                flat_props.update(flatten_properties(value, 'golem'))
            # Handle @tag pattern
            elif '@tag' in value:
                tag_value = value['@tag']
                flat_props[new_key] = tag_value
                # The tag's value becomes part of the prefix for the rest of the items
                tag_content = value.get(tag_value, {})
                if isinstance(tag_content, dict):
                    flat_props.update(flatten_properties(tag_content, f"{new_key}.{tag_value}"))
                else:
                    # In case the structure is not as expected, avoid crashing
                    print(f"Warning: @tag structure for {new_key} is not a dict.")
            else:
                flat_props.update(flatten_properties(value, new_key))
        else:
            flat_props[new_key] = value
    return flat_props

def transform_golem_base_offer(offer_data):
    """
    Transforms a single offer from golem-base format to the format expected by update_providers_info.
    """
    try:
        # Decode the base64 'value' field
        decoded_value_bytes = base64.b64decode(offer_data['value'])
        decoded_value_str = decoded_value_bytes.decode('utf-8')
        value_json = json.loads(decoded_value_str)

        # Flatten the properties
        properties = flatten_properties(value_json.get('properties', {}))
        
        # Add other top-level fields from the decoded value
        properties['golem.com.cgroup.version'] = value_json.get('cgroupVersion', 'unknown')
        properties['golem.com.expiration'] = value_json.get('expiration', 'unknown')

        # Map providerId to node_id
        properties['node_id'] = value_json.get('providerId')

        # Identify wallet and network
        wallet, network = identify_wallet_and_network(properties)
        properties['wallet'] = wallet
        properties['network'] = network

        return properties
    except (json.JSONDecodeError, KeyError, TypeError, base64.binascii.Error) as e:
        print(f"Error transforming golem-base offer: {e}")
        return None

@app.task
def golem_base_offer_scraper():
    """
    Fetches offers from the golem-base RPC endpoint, transforms them,
    and passes them to the update_providers_info task.
    """
    url = 'https://marketplace.holesky.golem-base.io/rpc'
    headers = {'Content-Type': 'application/json'}
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "golembase_queryEntities",
        "params": ["golem_marketplace_type=\"Offer\""]
    }

    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        offers = response.json().get('result', [])
        
        transformed_offers = []
        for offer in offers:
            transformed = transform_golem_base_offer(offer)
            if transformed:
                transformed_offers.append(json.dumps(transformed))
        
        if transformed_offers:
            print(f"Found {len(transformed_offers)} offers from golem-base. Sending to update_providers_info.")
            update_providers_info.delay(transformed_offers)

    except requests.exceptions.RequestException as e:
        print(f"Error fetching offers from golem-base: {e}")


examples_dir = pathlib.Path(__file__).resolve().parent.parent
sys.path.append(str(examples_dir))
from .yapapi_utils import build_parser, print_env_info, format_usage  # noqa: E402


async def list_offers(
    conf: Configuration, subnet_tag: str, current_scan_providers, node_props
):
    async with conf.market() as client:
        market_api = Market(client)
        dbuild = DemandBuilder()
        dbuild.add(yp.NodeInfo(
            name="some scanning node", subnet_tag=subnet_tag))
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
                Configuration(api_config=ApiConfig(
                    app_key="stats"
                )),
                subnet_tag=subnet_tag,
                node_props=node_props,
                current_scan_providers=current_scan_providers,
            ),
            timeout=60,  # 60-second timeout for each scan
        )
    except asyncio.TimeoutError:
        print("Scan timeout reached")
    print(
        f"In the current scan, we found {len(current_scan_providers)} providers")
# Delay update_nodes_data call using Celery

    update_providers_info.delay(node_props)
