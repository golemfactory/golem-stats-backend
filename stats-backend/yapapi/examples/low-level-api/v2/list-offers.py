#!/usr/bin/env python3
import asyncio
from asyncio import TimeoutError
from datetime import datetime, timezone
import json
import sys

from yapapi import props as yp
from yapapi.log import enable_default_logger
from yapapi.props.builder import DemandBuilder
from yapapi.rest import Configuration, Market, Activity, Payment  # noqa
import redis


data = []

test = []


async def list_offers(conf: Configuration, subnet_tag: str):
    async with conf.market() as client:
        market_api = Market(client)
        dbuild = DemandBuilder()
        dbuild.add(yp.NodeInfo(
            name="Golem Stats Indexer", subnet_tag=subnet_tag))
        dbuild.add(yp.Activity(expiration=datetime.now(timezone.utc)))
        async with market_api.subscribe(dbuild.properties, dbuild.constraints) as subscription:
            async for event in subscription.events():
                data = event.props
                if "golem.com.payment.platform.zksync-mainnet-glm.address" in str(event.props):
                    data['wallet'] = event.props['golem.com.payment.platform.zksync-mainnet-glm.address']
                elif "golem.com.payment.platform.zksync-rinkeby-tglm.address" in str(event.props):
                    data['wallet'] = event.props['golem.com.payment.platform.zksync-rinkeby-tglm.address']
                elif "golem.com.payment.platform.erc20-mainnet-glm.address" in str(event.props):
                    data['wallet'] = event.props['golem.com.payment.platform.erc20-mainnet-glm.address']
                elif "golem.com.payment.platform.erc20-polygon-glm.address" in str(event.props):
                    data['wallet'] = event.props['golem.com.payment.platform.erc20-polygon-glm.address']
                elif "golem.com.payment.platform.erc20-rinkeby-tglm.address" in str(event.props):
                    data['wallet'] = event.props['golem.com.payment.platform.erc20-rinkeby-tglm.address']
                elif "golem.com.payment.platform.polygon-polygon-glm.address" in str(event.props):
                    data['wallet'] = event.props['golem.com.payment.platform.polygon-polygon-glm.address']
                data['id'] = event.issuer
                test.append(json.dumps(data))


def main():
    try:
        asyncio.get_event_loop().run_until_complete(
            asyncio.wait_for(
                list_offers(
                    Configuration(),
                    subnet_tag="public-beta",
                ),
                timeout=15,
            )
        )
    except TimeoutError:
        pass

    try:
        asyncio.get_event_loop().run_until_complete(
            asyncio.wait_for(
                list_offers(
                    Configuration(),
                    subnet_tag="devnet-beta.1",
                ),
                timeout=15,
            )
        )
    except TimeoutError:
        pass
    try:
        asyncio.get_event_loop().run_until_complete(
            asyncio.wait_for(
                list_offers(
                    Configuration(),
                    subnet_tag="aarch64-network",
                ),
                timeout=15,
            )
        )
    except TimeoutError:
        pass
    try:
        asyncio.get_event_loop().run_until_complete(
            asyncio.wait_for(
                list_offers(
                    Configuration(),
                    subnet_tag="LazySubnet",
                ),
                timeout=15,
            )
        )
    except TimeoutError:
        pass
    try:
        asyncio.get_event_loop().run_until_complete(
            asyncio.wait_for(
                list_offers(
                    Configuration(),
                    subnet_tag="devnet-beta.2",
                ),
                timeout=15,
            )
        )
    except TimeoutError:
        pass
    try:
        asyncio.get_event_loop().run_until_complete(
            asyncio.wait_for(
                list_offers(
                    Configuration(),
                    subnet_tag="devnet-beta",
                ),
                timeout=15,
            )
        )
    except TimeoutError:
        pass
    print(len(test))
    if len(test) > 0:
        serialized = json.dumps(test)
        r = redis.Redis(host='redis', port=6379, db=0)
        content = r.set("offers_v2", serialized)


if __name__ == "__main__":
    main()
