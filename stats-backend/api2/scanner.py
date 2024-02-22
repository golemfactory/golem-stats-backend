#!/usr/bin/env python3
import asyncio
import csv
import json
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


@app.task(queue="yagna", options={"queue": "yagna", "routing_key": "yagna"})
def update_providers_info(node_props):
    unique_providers = set()  # Initialize a set to track unique providers
    now = timezone.now()
    days_in_current_month = calendar.monthrange(now.year, now.month)[1]
    seconds_current_month = days_in_current_month * 24 * 60 * 60
    glm_usd_value = GLM.objects.get(id=1)
    print(f"Updating {len(node_props)} providers")
    for prop in node_props:
        data = json.loads(prop)
        provider_id = data["node_id"]
        wallet = data["wallet"]
        unique_providers.add(provider_id)  # Add provider to the set
        obj, created = Node.objects.get_or_create(node_id=provider_id)
        if created:
            print(f"Created new provider: {prop}")
            offerobj = Offer.objects.create(
                properties=data, provider=obj, runtime=data["golem.runtime.name"]
            )
            if data["golem.runtime.name"] == "vm":
                vectors = {}
                for key, value in enumerate(data["golem.com.usage.vector"]):
                    vectors[value] = key
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
                offerobj.monthly_price_glm = monthly_pricing
                offerobj.monthly_price_usd = (
                    monthly_pricing * glm_usd_value.current_price
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

                # Compare and update the Offer object
                if closest_ec2 and monthly_pricing:
                    offer_price_usd = monthly_pricing * glm_usd_value.current_price
                    ec2_monthly_price = closest_ec2.price_usd * 730

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

                    # Update Offer object fields for cheaper comparison
                    offerobj.cheaper_than = closest_ec2 if offer_is_cheaper else None
                    offerobj.times_cheaper = (
                        float(ec2_monthly_price) / offer_price_usd
                        if offer_is_cheaper
                        else None
                    )

                else:
                    # print(
                    #     "No matching EC2Instance found or monthly pricing is not available."
                    # )
                    offerobj.is_overpriced = False
                    offerobj.overpriced_compared_to = None
                offerobj.save()
            obj.wallet = wallet
            # Verify each node's status
            is_online = check_node_status(obj.node_id)

            obj.online = is_online
            obj.save()
        else:
            offerobj, offercreated = Offer.objects.get_or_create(
                provider=obj, runtime=data["golem.runtime.name"]
            )
            if data["golem.runtime.name"] == "vm":
                vectors = {}
                for key, value in enumerate(data["golem.com.usage.vector"]):
                    vectors[value] = key
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
                offerobj.monthly_price_glm = monthly_pricing
                offerobj.monthly_price_usd = (
                    monthly_pricing * glm_usd_value.current_price
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

                # Compare and update the Offer object
                if closest_ec2 and monthly_pricing:
                    offer_price_usd = monthly_pricing * glm_usd_value.current_price
                    ec2_monthly_price = closest_ec2.price_usd * 730

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

                    # Update Offer object fields for cheaper comparison
                    offerobj.cheaper_than = closest_ec2 if offer_is_cheaper else None
                    offerobj.times_cheaper = (
                        float(ec2_monthly_price) / offer_price_usd
                        if offer_is_cheaper
                        else None
                    )

                else:
                    # print(
                    #     "No matching EC2Instance found or monthly pricing is not available."
                    # )
                    offerobj.is_overpriced = False
                    offerobj.overpriced_compared_to = None

            offerobj.properties = data
            offerobj.save()
            obj.runtime = data["golem.runtime.name"]
            obj.wallet = wallet
            # Verify each node's status
            is_online = check_node_status(obj.node_id)
            obj.online = is_online
            obj.save()

    online_nodes = Node.objects.filter(online=True)
    for node in online_nodes:
        if not node.node_id in unique_providers:
            command = f"yagna net find {node.node_id}"
            result = subprocess.run(command, shell=True, capture_output=True, text=True)
            is_online = "Exiting..., error details: Request failed" not in result.stderr
            node.online = is_online
            node.computing_now = False
            node.save(update_fields=["online", "computing_now"])
    print(f"Done updating {len(unique_providers)} providers")


TESTNET_KEYS = [
    "golem.com.payment.platform.erc20-goerli-tglm.address",
    "golem.com.payment.platform.erc20-mumbai-tglm.address",
    "golem.com.payment.platform.erc20-holesky-tglm.address",
    "golem.com.payment.platform.erc20next-goerli-tglm.address",
    "golem.com.payment.platform.erc20next-mumbai-tglm.address",
    "golem.com.payment.platform.erc20next-holesky-tglm.address",
]

examples_dir = pathlib.Path(__file__).resolve().parent.parent
sys.path.append(str(examples_dir))
from .yapapi_utils import build_parser, print_env_info, format_usage  # noqa: E402

import redis


def update_nodes_status(provider_id, is_online_now):
    provider, created = Node.objects.get_or_create(node_id=provider_id)

    # Check the last status in the NodeStatusHistory
    last_status = NodeStatusHistory.objects.filter(provider=provider).last()

    if not last_status or last_status.is_online != is_online_now:
        # Create a new status entry if there's a change in status
        NodeStatusHistory.objects.create(provider=provider, is_online=is_online_now)


@app.task(queue="yagna", options={"queue": "yagna", "routing_key": "yagna"})
def update_nodes_data(node_props):
    r = redis.Redis(host="redis", port=6379, db=0)

    for props in node_props:
        props = json.loads(props)
        issuer_id = props["node_id"]
        is_online_now = check_node_status(issuer_id)
        try:
            update_nodes_status(issuer_id, is_online_now)
            r.set(f"provider:{issuer_id}:status", str(is_online_now))
        except Exception as e:
            print(f"Error updating NodeStatus for {issuer_id}: {e}")
    print(f"Done updating {len(node_props)} providers")
    # Deserialize each element in node_props into a dictionary
    deserialized_node_props = [json.loads(props) for props in node_props]

    # Now create the set
    provider_ids_in_props = {props["node_id"] for props in deserialized_node_props}
    previously_online_providers_ids = (
        Node.objects.filter(nodestatushistory__is_online=True)
        .distinct()
        .values_list("node_id", flat=True)
    )

    provider_ids_not_in_scan = (
        set(previously_online_providers_ids) - provider_ids_in_props
    )

    for issuer_id in provider_ids_not_in_scan:
        is_online_now = check_node_status(issuer_id)

        try:
            update_nodes_status(issuer_id, is_online_now)
            r.set(f"provider:{issuer_id}:status", str(is_online_now))
        except Exception as e:
            print(f"Error verifying/updating NodeStatus for {issuer_id}: {e}")
    print(f"Done updating {len(provider_ids_not_in_scan)} OFFLINE providers")


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
                if "golem.com.payment.platform.zksync-mainnet-glm.address" in str(
                    event.props
                ):
                    data["wallet"] = event.props[
                        "golem.com.payment.platform.zksync-mainnet-glm.address"
                    ]
                elif "golem.com.payment.platform.zksync-rinkeby-tglm.address" in str(
                    event.props
                ):
                    data["wallet"] = event.props[
                        "golem.com.payment.platform.zksync-rinkeby-tglm.address"
                    ]
                elif "golem.com.payment.platform.erc20-mainnet-glm.address" in str(
                    event.props
                ):
                    data["wallet"] = event.props[
                        "golem.com.payment.platform.erc20-mainnet-glm.address"
                    ]
                elif "golem.com.payment.platform.erc20-polygon-glm.address" in str(
                    event.props
                ):
                    data["wallet"] = event.props[
                        "golem.com.payment.platform.erc20-polygon-glm.address"
                    ]
                elif "golem.com.payment.platform.erc20-goerli-tglm.address" in str(
                    event.props
                ):
                    data["wallet"] = event.props[
                        "golem.com.payment.platform.erc20-goerli-tglm.address"
                    ]
                elif "golem.com.payment.platform.erc20-rinkeby-tglm.address" in str(
                    event.props
                ):
                    data["wallet"] = event.props[
                        "golem.com.payment.platform.erc20-rinkeby-tglm.address"
                    ]
                elif "golem.com.payment.platform.polygon-polygon-glm.address" in str(
                    event.props
                ):
                    data["wallet"] = event.props[
                        "golem.com.payment.platform.polygon-polygon-glm.address"
                    ]
                elif "golem.com.payment.platform.erc20next-mainnet-glm.address" in str(
                    event.props
                ):
                    data["wallet"] = event.props[
                        "golem.com.payment.platform.erc20next-mainnet-glm.address"
                    ]
                elif "golem.com.payment.platform.erc20next-polygon-glm.address" in str(
                    event.props
                ):
                    data["wallet"] = event.props[
                        "golem.com.payment.platform.erc20next-polygon-glm.address"
                    ]
                elif "golem.com.payment.platform.erc20next-goerli-tglm.address" in str(
                    event.props
                ):
                    data["wallet"] = event.props[
                        "golem.com.payment.platform.erc20next-goerli-tglm.address"
                    ]
                elif "golem.com.payment.platform.erc20next-rinkeby-tglm.address" in str(
                    event.props
                ):
                    data["wallet"] = event.props[
                        "golem.com.payment.platform.erc20next-rinkeby-tglm.address"
                    ]
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
