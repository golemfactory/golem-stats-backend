from __future__ import absolute_import, unicode_literals
import os
from celery import Celery
import logging
from celery.schedules import crontab


logger = logging.getLogger("Celery")

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'core.settings')

app = Celery('core')


@app.on_after_finalize.connect
def setup_periodic_tasks(sender, **kwargs):
    from collector.tasks import offer_scraper
    sender.add_periodic_task(
        crontab(minute="*/1"),
        offer_scraper.s(),
    )


app.conf.broker_url = 'redis://redis:6379/0'
app.conf.result_backend = 'redis://redis:6379/0'
