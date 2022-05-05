from core.celery import app
from celery import Celery
import json
import subprocess
import os
from .models import Node, Offer
from django.utils import timezone
import tempfile
import redis
from .serializers import NodeSerializer
import calendar
import datetime

pool = redis.ConnectionPool(host='redis', port=6379, db=0)
r = redis.Redis(connection_pool=pool)


@ app.task
def v2_network_online_to_redis():
    data = Node.objects.filter(online=True)
    serializer = NodeSerializer(data, many=True)
    test = json.dumps(serializer.data, default=str)

    r.set("v2_online", test)


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
