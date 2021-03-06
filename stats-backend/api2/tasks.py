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
    two_cores = [{'name': 'Digital Ocean', 'img': '/do-logo.svg',
                  'usd_monthly': '15',  'bandwidth': '3', 'cores': 2, 'memory': '1', 'disk': "25", "glm": float(price) * 15}, {'name': 'Amazon Web Services', 'img': '/aws-logo.svg',
                                                                                                                               'usd_monthly': '15.23', 'bandwidth': 'Unlimited', 'cores': 2, 'memory': '1', 'disk': "25", "glm": float(price) * 15.23}, {'name': 'Google Cloud Platform', 'img': '/gcp-logo.svg',
                                                                                                                                                                                                                                                         'usd_monthly': '10.37', 'bandwidth': 'Unlimited', 'cores': 2, 'memory': '1', 'disk': "25", "glm": float(price) * 10.37}, {'name': 'Azure', 'img': '/azure-logo.svg',
                                                                                                                                                                                                                                                                                                                                                                                   'usd_monthly': '15.11', 'bandwidth': '6', 'cores': 2, 'memory': '1', 'disk': "25", "glm": float(price) * 15.11}, ]
    eight_cores = [{'name': 'Digital Ocean', 'img': '/do-logo.svg',
                    'usd_monthly': '80',  'bandwidth': '6', 'cores': 8, 'memory': '16', 'disk': "320", "glm": float(price) * 80}, {'name': 'Amazon Web Services', 'img': '/aws-logo.svg',
                                                                                                                                   'usd_monthly': '121.81', 'bandwidth': 'Unlimited', 'cores': 8, 'memory': '16', 'disk': "320", "glm": float(price) * 121.81}, {'name': 'Google Cloud Platform', 'img': '/gcp-logo.svg',
                                                                                                                                                                                                                                                                 'usd_monthly': '208.47', 'bandwidth': 'Unlimited', 'cores': 8, 'memory': '32', 'disk': "320", "glm": float(price) * 208.47}, {'name': 'Azure', 'img': '/azure-logo.svg',
                                                                                                                                                                                                                                                                                                                                                                                               'usd_monthly': '121.18', 'cores': 8, 'memory': '16', 'bandwidth': '6', 'disk': "320", "glm": float(price) * 121.18}]
    thirtytwo_cores = [{'name': 'Digital Ocean', 'img': '/do-logo.svg',
                        'usd_monthly': '640',  'bandwidth': '9', 'cores': 32, 'memory': '64', 'disk': "400", "glm": float(price) * 640}, {'name': 'Amazon Web Services', 'img': '/aws-logo.svg',
                                                                                                                                          'usd_monthly': '834.24', 'bandwidth': 'Unlimited', 'cores': 32, 'memory': '64', 'disk': "400", "glm": float(price) * 834.24}, {'name': 'Google Cloud Platform', 'img': '/gcp-logo.svg',
                                                                                                                                                                                                                                                                         'usd_monthly': '746.04', 'bandwidth': 'Unlimited', 'cores': 32, 'memory': '64', 'disk': "400", "glm": float(price) * 746.04}, {'name': 'Azure', 'img': '/azure-logo.svg',
                                                                                                                                                                                                                                                                                                                                                                                                        'usd_monthly': '1310.13', 'bandwidth': '1', 'cores': 32, 'memory': '64', 'disk': "256", "glm": float(price) * 1310.13}, ]
    sixtyfour_cores = [{'name': 'Digital Ocean', 'img': '/do-logo.svg',
                        'usd_monthly': '1200',  'bandwidth': '9', 'cores': 40, 'memory': '160', 'disk': "500", "glm": float(price) * 1200}, {'name': 'Amazon Web Services', 'img': '/aws-logo.svg',
                                                                                                                                             'usd_monthly': '1638.48', 'bandwidth': 'Unlimited', 'cores': 64, 'memory': '64', 'disk': "500", "glm": float(price) * 1638.48}, {'name': 'Google Cloud Platform', 'img': '/gcp-logo.svg',
                                                                                                                                                                                                                                                                              'usd_monthly': '1914.62', 'bandwidth': 'Unlimited', 'cores': 60, 'memory': '240', 'disk': "500", "glm": float(price) * 1914.62}, {'name': 'Azure', 'img': '/azure-logo.svg',
                                                                                                                                                                                                                                                                                                                                                                                                                'usd_monthly': '2688.37', 'bandwidth': '1', 'cores': 64, 'memory': '256', 'disk': "512", "glm": float(price) * 2688.37}, ]
    for obj in sorted_pricing_and_specs:
        provider = {}
        provider['name'] = "Golem Network"
        provider['node_id'] = obj['properties']['id']
        provider['img'] = "/golem.png"
        provider['usd_monthly'] = float(
            price) * float(obj['monthly_price_glm'])
        provider['cores'] = float(
            obj['properties']['golem.inf.cpu.threads'])
        provider['memory'] = float(obj['properties']['golem.inf.mem.gib'])
        provider['bandwidth'] = "Unlimited"
        provider['disk'] = float(
            obj['properties']['golem.inf.storage.gib'])
        provider['glm'] = float(obj['monthly_price_glm'])
        if float(obj['properties']['golem.inf.cpu.threads']) == 2 and len(two_cores) == 4:
            two_cores.append(provider)
        elif float(obj['properties']['golem.inf.cpu.threads']) >= 2 and len(two_cores) == 4:

            two_cores.append(provider)
        if float(obj['properties']['golem.inf.cpu.threads']) == 8 and len(eight_cores) == 4:

            eight_cores.append(provider)
        elif float(obj['properties']['golem.inf.cpu.threads']) >= 8 and len(eight_cores) == 4:

            eight_cores.append(provider)
        if float(obj['properties']['golem.inf.cpu.threads']) == 32 and len(thirtytwo_cores) == 4:

            thirtytwo_cores.append(provider)
        elif float(obj['properties']['golem.inf.cpu.threads']) >= 32 and len(thirtytwo_cores) == 4:

            thirtytwo_cores.append(provider)
        if float(obj['properties']['golem.inf.cpu.threads']) == 64 and len(sixtyfour_cores) == 4:

            sixtyfour_cores.append(provider)
        elif float(obj['properties']['golem.inf.cpu.threads']) >= 64 and len(sixtyfour_cores) == 4:

            sixtyfour_cores.append(provider)

    sorted_two = sorted(two_cores, key=lambda element: (
        float(element['usd_monthly'])))
    sorted_eight = sorted(eight_cores, key=lambda element: (
        float(element['usd_monthly'])))
    sorted_thirtytwo = sorted(thirtytwo_cores, key=lambda element: (
        float(element['usd_monthly'])))
    sorted_sixtyfour = sorted(sixtyfour_cores, key=lambda element: (
        float(element['usd_monthly'])))
    data = json.dumps({'2': sorted_two, '8': sorted_eight,
                      '32': sorted_thirtytwo, '64': sorted_sixtyfour})
    r.set("v2_cheapest_provider", data)


@ app.task
def v2_offer_scraper():
    os.chdir("/stats-backend/yapapi/examples/low-level-api/v2")
    with open('data.config') as f:
        for line in f:
            command = line
    proc = subprocess.Popen(command, shell=True)
    proc.wait()
    content = r.get("offers_v2")
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
