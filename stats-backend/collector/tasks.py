from core.celery import app
from celery import Celery
import json
import subprocess
import os
from django.db import transaction
from .models import Node


@app.task
def offer_scraper():
    os.chdir("/stats-backend/yapapi/examples/low-level-api")
    with open('data.config') as f:
        for line in f:
            command = line
    proc = subprocess.Popen(command, shell=True)
    proc.wait()
    with open('data.json') as f:
        for line in f:
            if not line.strip():
                continue
            data = json.loads(line)
            provider = data['id']
            obj, created = Node.objects.get_or_create(node_id=provider)
            if created:
                obj.data = data
                obj.online = True
                obj.save()
            else:
                obj.data = data
                obj.online = True
                obj.save()
    # Find offline providers
    online_nodes = Node.objects.filter(online=True)
    for node in online_nodes:
        if not node.node_id in open('data.json').read():
            node.online = False
            node.save()
    os.remove("data.json")
    open("data.json", 'a').close()
