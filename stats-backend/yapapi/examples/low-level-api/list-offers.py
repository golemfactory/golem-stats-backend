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
from yapapi.config import ApiConfig


data = []

test = []


async def list_offers(conf: Configuration, subnet_tag: str):
    async with conf.market() as client:
        market_api = Market(client)
        dbuild = DemandBuilder()
        dbuild.add(yp.NodeInfo(
            name="Golem Stats Indexer", subnet_tag=subnet_tag))
        dbuild.add(yp.Activity(expiration=datetime.now(timezone.utc)))
        async with market_api.subscribe(
            dbuild.properties, dbuild.constraints
        ) as subscription:
            async for event in subscription.events():
                if event.props["golem.runtime.name"] == "vm":
                    if event.issuer in str(test):
                        continue
                    else:
                        data = event.props
                        if event.props["golem.runtime.name"] == "gminer":
                            try:
                                data["wallet"] = event.props[
                                    "golem.com.payment.platform.polygon-polygon-glm.address"
                                ]
                            except:
                                data["wallet"] = event.props[
                                    "golem.com.payment.platform.erc20-polygon-glm.address"
                                ]

                            data["golem.node.debug.subnet"] = "Thorg"
                            data["id"] = event.issuer
                            test.append(json.dumps(data))
                        if event.props["golem.runtime.name"] == "hminer":
                            data["wallet"] = event.props[
                                "golem.com.payment.platform.polygon-polygon-glm.address"
                            ]
                            data["golem.node.debug.subnet"] = "Thorg"
                            data["id"] = event.issuer
                            test.append(json.dumps(data))
                        if (
                            "golem.com.payment.platform.zksync-mainnet-glm.address"
                            in str(event.props)
                        ):
                            data["wallet"] = event.props[
                                "golem.com.payment.platform.zksync-mainnet-glm.address"
                            ]
                        elif (
                            "golem.com.payment.platform.zksync-rinkeby-tglm.address"
                            in str(event.props)
                        ):
                            data["wallet"] = event.props[
                                "golem.com.payment.platform.zksync-rinkeby-tglm.address"
                            ]
                        elif (
                            "golem.com.payment.platform.erc20-goerli-tglm.address"
                            in str(event.props)
                        ):
                            data["wallet"] = event.props[
                                "golem.com.payment.platform.erc20-goerli-tglm.address"
                            ]
                        elif (
                            "golem.com.payment.platform.erc20-mainnet-glm.address"
                            in str(event.props)
                        ):
                            data["wallet"] = event.props[
                                "golem.com.payment.platform.erc20-mainnet-glm.address"
                            ]
                        elif (
                            "golem.com.payment.platform.erc20-polygon-glm.address"
                            in str(event.props)
                        ):
                            data["wallet"] = event.props[
                                "golem.com.payment.platform.erc20-polygon-glm.address"
                            ]
                        elif (
                            "golem.com.payment.platform.erc20-rinkeby-tglm.address"
                            in str(event.props)
                        ):
                            data["wallet"] = event.props[
                                "golem.com.payment.platform.erc20-rinkeby-tglm.address"
                            ]
                        elif (
                            "golem.com.payment.platform.erc20next-mainnet-glm.address"
                            in str(event.props)
                        ):
                            data["wallet"] = event.props[
                                "golem.com.payment.platform.erc20next-mainnet-glm.address"
                            ]
                        elif (
                            "golem.com.payment.platform.erc20next-polygon-glm.address"
                            in str(event.props)
                        ):
                            data["wallet"] = event.props[
                                "golem.com.payment.platform.erc20next-polygon-glm.address"
                            ]
                        elif (
                            "golem.com.payment.platform.erc20next-goerli-tglm.address"
                            in str(event.props)
                        ):
                            data["wallet"] = event.props[
                                "golem.com.payment.platform.erc20next-goerli-tglm.address"
                            ]
                        elif (
                            "golem.com.payment.platform.erc20next-rinkeby-tglm.address"
                            in str(event.props)
                        ):
                            data["wallet"] = event.props[
                                "golem.com.payment.platform.erc20next-rinkeby-tglm.address"
                            ]
                        data["id"] = event.issuer
                        test.append(json.dumps(data))


def main():
    try:
        asyncio.get_event_loop().run_until_complete(
            asyncio.wait_for(
                list_offers(
                    Configuration(api_config=ApiConfig(
                        app_key="stats"
                    )),  # YAGNA_APPKEY will be loaded from env
                    subnet_tag="public",
                ),
                timeout=60,
            )
        )
    except TimeoutError:
        pass
    serialized = json.dumps(test)
    r = redis.Redis(host="redis", port=6379, db=0)
    content = r.set("v1_offers", serialized)


if __name__ == "__main__":
    main()
