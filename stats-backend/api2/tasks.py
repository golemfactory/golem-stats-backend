from core.celery import app
from celery import Celery
import json
import subprocess
import os
from .models import Node, Offer
from django.utils import timezone
import tempfile
import redis
from .serializers import NodeSerializer, OfferSerializer
import calendar
import datetime
import requests

pool = redis.ConnectionPool(host='redis', port=6379, db=0)
r = redis.Redis(connection_pool=pool)


@ app.task
def v2_network_online_to_redis():
    data = Node.objects.filter(online=True)
    serializer = NodeSerializer(data, many=True)
    test = json.dumps(serializer.data, default=str)

    r.set("v2_online", test)


@ app.task
def latest_blog_posts():
    req = requests.get(
        f"https://blog.golemproject.net/ghost/api/v3/content/posts/?key={os.environ.get('BLOG_API_KEY')}&include=tags,authors&limit=3")
    data = json.dumps(req.json())
    r.set("v2_index_blog_posts", data)


@ app.task
def v2_cheapest_provider():
    req = requests.get(
        "https://api.coingecko.com/api/v3/coins/ethereum/contract/0x7DD9c5Cba05E151C895FDe1CF355C9A1D5DA6429")
    data = req.json()
    price = data['market_data']['current_price']['usd']
    obj = Offer.objects.filter(runtime="vm").order_by("monthly_price_glm")
    serializer = OfferSerializer(obj, many=True)
    mainnet_providers = []
    for index, provider in enumerate(serializer.data):
        if "golem.com.payment.platform.erc20-mainnet-glm.address" in provider['properties']:
            mainnet_providers.append(provider)
    sorted_pricing_and_specs = sorted(mainnet_providers, key=lambda element: (
        float(element['properties']['golem.inf.cpu.threads']), float(element['monthly_price_glm'])))
    two_cores = []
    eight_cores = []
    thirtytwo_cores = []
    sixtyfour_cores = []
    for obj in sorted_pricing_and_specs:
        if float(obj['properties']['golem.inf.cpu.threads']) == 2:
            obj['usd_monthly'] = float(price) * float(obj['monthly_price_glm'])
            obj['active'] = True
            two_cores.append(obj)
        elif float(obj['properties']['golem.inf.cpu.threads']) >= 2:
            obj['usd_monthly'] = float(price) * float(obj['monthly_price_glm'])
            obj['active'] = True
            two_cores.append(obj)
        if float(obj['properties']['golem.inf.cpu.threads']) == 8:
            obj['active'] = False
            obj['usd_monthly'] = float(price) * float(obj['monthly_price_glm'])
            eight_cores.append(obj)
        elif float(obj['properties']['golem.inf.cpu.threads']) >= 8:
            obj['active'] = False
            obj['usd_monthly'] = float(price) * float(obj['monthly_price_glm'])
            eight_cores.append(obj)
        if float(obj['properties']['golem.inf.cpu.threads']) == 32:
            obj['active'] = False
            obj['usd_monthly'] = float(price) * float(obj['monthly_price_glm'])
            thirtytwo_cores.append(obj)
        elif float(obj['properties']['golem.inf.cpu.threads']) >= 32:
            obj['active'] = False
            obj['usd_monthly'] = float(price) * float(obj['monthly_price_glm'])
            thirtytwo_cores.append(obj)
        if float(obj['properties']['golem.inf.cpu.threads']) == 64:
            obj['active'] = False
            obj['usd_monthly'] = float(price) * float(obj['monthly_price_glm'])
            sixtyfour_cores.append(obj)
        elif float(obj['properties']['golem.inf.cpu.threads']) >= 64:
            obj['active'] = False
            obj['usd_monthly'] = float(price) * float(obj['monthly_price_glm'])
            sixtyfour_cores.append(obj)
    data = json.dumps({'2': two_cores[0], '8': eight_cores[0],
                      '32': thirtytwo_cores[0], '64': sixtyfour_cores[0]})
    r.set("v2_cheapest_provider", data)


@ app.task
def v2_offer_scraper():
    os.chdir("/stats-backend/yapapi/examples/low-level-api/v2")
    with open('data.config') as f:
        for line in f:
            command = line
    proc = subprocess.Popen(command, shell=True)
    proc.wait()
    content = r.get("offers")
    serialized = json.loads(content)
    now = datetime.datetime.now()
    days_in_current_month = calendar.monthrange(
        now.year, now.month)[1]
    seconds_current_month = days_in_current_month*24*60*60
    for line in serialized:
        data = json.loads(line)
        provider = data['id']
        wallet = data['wallet']
        obj, created = Node.objects.get_or_create(node_id=provider)
        if created:
            offerobj = Offer.objects.create(properties=data, provider=obj,
                                            runtime=data['golem.runtime.name'])
            if data['golem.runtime.name'] == 'vm':
                vectors = {}
                for key, value in enumerate(data['golem.com.usage.vector']):
                    vectors[value] = key
                monthly_pricing = (data['golem.com.pricing.model.linear.coeffs'][vectors['golem.usage.duration_sec']] * seconds_current_month) + (
                    data['golem.com.pricing.model.linear.coeffs'][vectors['golem.usage.cpu_sec']] * seconds_current_month * data['golem.inf.cpu.cores']) + data['golem.com.pricing.model.linear.coeffs'][-1]
                offerobj.monthly_price_glm = monthly_pricing
                offerobj.save()
            obj.wallet = wallet
            obj.online = True
            obj.save()
        else:
            offerobj, offercreated = Offer.objects.get_or_create(
                provider=obj, runtime=data['golem.runtime.name'])
            if data['golem.runtime.name'] == 'vm':
                vectors = {}
                for key, value in enumerate(data['golem.com.usage.vector']):
                    vectors[value] = key
                monthly_pricing = (data['golem.com.pricing.model.linear.coeffs'][vectors['golem.usage.duration_sec']] * seconds_current_month) + (
                    data['golem.com.pricing.model.linear.coeffs'][vectors['golem.usage.cpu_sec']] * seconds_current_month * data['golem.inf.cpu.cores']) + data['golem.com.pricing.model.linear.coeffs'][-1]
                offerobj.monthly_price_glm = monthly_pricing
                offerobj.save()
            offerobj.properties = data
            offerobj.save()
            obj.runtime = data['golem.runtime.name']
            obj.wallet = wallet
            obj.online = True
            obj.save()
    # Find offline providers
    str1 = ''.join(serialized)
    fd, path = tempfile.mkstemp()
    try:
        with os.fdopen(fd, 'w') as tmp:
            # do stuff with temp file
            tmp.write(str1)
            online_nodes = Node.objects.filter(online=True)
            for node in online_nodes:
                if not node.node_id in str1:
                    node.online = False
                    node.computing_now = False
                    node.save(update_fields=[
                              'online', 'computing_now'])
    finally:
        os.remove(path)
