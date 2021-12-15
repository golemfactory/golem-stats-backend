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
            name="some scanning node", subnet_tag=subnet_tag))
        dbuild.add(yp.Activity(expiration=datetime.now(timezone.utc)))
        async with market_api.subscribe(dbuild.properties, dbuild.constraints) as subscription:
            async for event in subscription.events():
                if event.props['golem.runtime.name'] != "vm":
                    continue
                if event.issuer in test:
                    continue
                else:
                    data = event.props
                    try:
                        data['wallet'] = event.props['golem.com.payment.platform.zksync-mainnet-glm.address']
                    except:
                        data['wallet'] = event.props['golem.com.payment.platform.zksync-rinkeby-tglm.address']
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
                timeout=4,
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
                timeout=4,
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
                timeout=4,
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
                timeout=4,
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
                timeout=4,
            )
        )
    except TimeoutError:
        pass
    serialized = json.dumps(test)
    if len(test) > 450:
        r = redis.Redis(host='redis', port=6379, db=0)
        content = r.set("offers", serialized)


if __name__ == "__main__":
    main()
