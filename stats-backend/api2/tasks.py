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
from decimal import Decimal
from .utils import get_pricing, get_ec2_products, find_cheapest_price, has_vcpu_memory, round_to_three_decimals

pool = redis.ConnectionPool(host="redis", port=6379, db=0)
r = redis.Redis(connection_pool=pool)


@app.task
def v2_network_online_to_redis():
    data = Node.objects.filter(online=True)
    serializer = NodeSerializer(data, many=True)
    test = json.dumps(serializer.data, default=str)

    r.set("v2_online", test)


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
        provider = {}
        provider["name"] = "Golem Network"
        provider["node_id"] = obj["properties"]["id"]
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
        price = str(data['market_data']['current_price']['usd'])[0:5]
        obj, created = GLM.objects.get_or_create(id=1)
        obj.current_price = price
        obj.save()
    else:
        print("Failed to retrieve data")


@app.task
def v2_offer_scraper():
    os.chdir("/stats-backend/yapapi/examples/low-level-api/v2")
    with open("data.config") as f:
        for line in f:
            command = line
    proc = subprocess.Popen(command, shell=True)
    proc.wait()
    content = r.get("offers_v2")
    serialized = json.loads(content)
    now = datetime.datetime.now()
    days_in_current_month = calendar.monthrange(now.year, now.month)[1]
    seconds_current_month = days_in_current_month * 24 * 60 * 60
    glm_usd_value = GLM.objects.get(id=1)
    for line in serialized:
        data = json.loads(line)
        provider = data["id"]
        wallet = data["wallet"]
        obj, created = Node.objects.get_or_create(node_id=provider)
        if created:
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
                offerobj.monthly_price_usd = monthly_pricing * glm_usd_value.current_price
                vcpu_needed = data.get("golem.inf.cpu.threads", 0)
                memory_needed = data.get("golem.inf.mem.gib", 0.0)
                closest_ec2 = EC2Instance.objects.annotate(
                    cpu_diff=Abs(F('vcpu') - vcpu_needed),
                    memory_diff=Abs(F('memory') - memory_needed)
                ).order_by('cpu_diff', 'memory_diff', 'price_usd').first()

                # Compare and update the Offer object
                if closest_ec2 and monthly_pricing:
                    offer_is_more_expensive = offerobj.monthly_price_usd > closest_ec2.price_usd
                    comparison_result = "more expensive" if offer_is_more_expensive else "cheaper"

                    # Update Offer object fields
                    offerobj.is_overpriced = offer_is_more_expensive
                    if offer_is_more_expensive:
                        offerobj.overpriced_compared_to = closest_ec2
                        offerobj.suggest_env_per_hour_price = round_to_three_decimals((
                        closest_ec2.price_usd /
                        Decimal(glm_usd_value.current_price) /
                        (seconds_current_month / Decimal(3600))
                        ))
                        offerobj.times_more_expensive = offerobj.monthly_price_usd / closest_ec2.price_usd
                    else:
                        offerobj.overpriced_compared_to = None
                    
                else:
                    print("No matching EC2Instance found or monthly pricing is not available.")
                    offerobj.is_overpriced = False
                    offerobj.overpriced_compared_to = None
                offerobj.save()
            obj.wallet = wallet
            # Verify each node's status
            command = f"yagna net find {provider}"
            result = subprocess.run(command, shell=True, capture_output=True, text=True)
            is_online = "Exiting..., error details: Request failed" not in result.stderr

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
                offerobj.monthly_price_usd = monthly_pricing * glm_usd_value.current_price
                
                
                vcpu_needed = data.get("golem.inf.cpu.threads", 0)
                memory_needed = data.get("golem.inf.mem.gib", 0.0)
                closest_ec2 = EC2Instance.objects.annotate(
                    cpu_diff=Abs(F('vcpu') - vcpu_needed),
                    memory_diff=Abs(F('memory') - memory_needed)
                ).order_by('cpu_diff', 'memory_diff', 'price_usd').first()

                # Compare and update the Offer object
                if closest_ec2 and monthly_pricing:
                    offer_is_more_expensive = offerobj.monthly_price_usd > closest_ec2.price_usd
                    comparison_result = "more expensive" if offer_is_more_expensive else "cheaper"

                    # Update Offer object fields
                    offerobj.is_overpriced = offer_is_more_expensive
                    if offer_is_more_expensive:
                        offerobj.overpriced_compared_to = closest_ec2
                        offerobj.suggest_env_per_hour_price = round_to_three_decimals((
                        closest_ec2.price_usd /
                        Decimal(glm_usd_value.current_price) /
                        (seconds_current_month / Decimal(3600))
                        ))
                        offerobj.times_more_expensive = offerobj.monthly_price_usd / closest_ec2.price_usd
                    else:
                        offerobj.overpriced_compared_to = None
                    
                else:
                    print("No matching EC2Instance found or monthly pricing is not available.")
                    offerobj.is_overpriced = False
                    offerobj.overpriced_compared_to = None
                    
            offerobj.properties = data
            offerobj.save()
            obj.runtime = data["golem.runtime.name"]
            obj.wallet = wallet
            # Verify each node's status
            command = f"yagna net find {provider}"
            result = subprocess.run(command, shell=True, capture_output=True, text=True)
            is_online = "Exiting..., error details: Request failed" not in result.stderr
            obj.online = is_online
            obj.save()
    # Find offline providers
    str1 = "".join(serialized)
    fd, path = tempfile.mkstemp()
    try:
        with os.fdopen(fd, "w") as tmp:
            # do stuff with temp file
            tmp.write(str1)
            online_nodes = Node.objects.filter(online=True)
            for node in online_nodes:
                if not node.node_id in str1:
                    command = f"yagna net find {node.node_id}"
                    result = subprocess.run(
                        command, shell=True, capture_output=True, text=True
                    )
                    is_online = (
                        "Exiting..., error details: Request failed" not in result.stderr
                    )
                    node.online = is_online
                    node.computing_now = False
                    node.save(update_fields=["online", "computing_now"])
    finally:
        os.remove(path)


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





@app.task
def store_ec2_info():
    ec2_info = {}
    products_data = get_ec2_products()

    for product in products_data:
        details = product.get('details', {})
        if not has_vcpu_memory(details):
            continue
        print(product)
        product_id = product['id']
        category = product.get('category')
        name = product.get('name')

        pricing_data = get_pricing(product_id)
        cheapest_price = find_cheapest_price(pricing_data['prices'])

        # Convert memory to float and price to Decimal
        memory_gb = float(details['memory'])
        price = cheapest_price['amount'] if cheapest_price else None

        # Use get_or_create to store or update the instance in the database
        instance, created = EC2Instance.objects.get_or_create(
            name=name,
            defaults={'vcpu': details['vcpu'], 'memory': memory_gb, 'price_usd': price}
        )

        ec2_info[product_id] = {
            'category': category,
            'name': name,
            'details': details,
            'cheapest_price': cheapest_price
        }

    return ec2_info
