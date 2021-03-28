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


data = []
jsonlist = {
    "offers": []
}
async def list_offers(conf: Configuration, subnet_tag: str):
    async with conf.market() as client:
        market_api = Market(client)
        dbuild = DemandBuilder()
        dbuild.add(yp.NodeInfo(name="some scanning node", subnet_tag=subnet_tag))
        dbuild.add(yp.Activity(expiration=datetime.now(timezone.utc)))

        async with market_api.subscribe(dbuild.properties, dbuild.constraints) as subscription:
            async for event in subscription.events():
                #data.append(f"{json.dumps(event.props)}")
                with open('data.json', 'a') as f:
                    f.write(json.dumps(event.props) + "\n")

async def list_offers_testnet(conf: Configuration, subnet_tag: str):
    async with conf.market() as client:
        market_api = Market(client)
        dbuild = DemandBuilder()
        dbuild.add(yp.NodeInfo(name="some scanning node", subnet_tag=subnet_tag))
        dbuild.add(yp.Activity(expiration=datetime.now(timezone.utc)))

        async with market_api.subscribe(dbuild.properties, dbuild.constraints) as subscription:
            async for event in subscription.events():
                #data.append(f"{json.dumps(event.props)}")
                with open('data.json', 'a') as f:
                    f.write(json.dumps(event.props) + "\n")
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

if __name__ == "__main__":
    main()
