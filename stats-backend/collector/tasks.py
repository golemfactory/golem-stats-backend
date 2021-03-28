from core.celery import app
from celery import Celery
import json
import subprocess
import os
from django.db import transaction


@app.task
def offer_scraper():
    os.chdir("/stats-backend/yapapi/examples/low-level-api")
    with open('data.config') as f:
        for line in f:
            command = line
    proc = subprocess.call(command, shell=True)
    with open('data.json') as f:
        for line in f:
            print(line)
    os.remove("data.json")