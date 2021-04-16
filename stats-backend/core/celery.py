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
    from collector.tasks import offer_scraper, network_online_to_redis, network_stats_to_redis, network_utilization_to_redis, computing_now_to_redis, providers_average_earnings_to_redis, network_earnings_6h_to_redis, network_earnings_24h_to_redis, network_versions_to_redis
    sender.add_periodic_task(
        30.0,
        offer_scraper.s(),
    )
    sender.add_periodic_task(
        10.0,
        network_online_to_redis.s(),
    )
    sender.add_periodic_task(
        10.0,
        network_stats_to_redis.s(),
    )
    sender.add_periodic_task(
        10.0,
        network_utilization_to_redis.s(),
    )
    sender.add_periodic_task(
        10.0,
        computing_now_to_redis.s(),
    )
    sender.add_periodic_task(
        10.0,
        providers_average_earnings_to_redis.s(),
    )
    sender.add_periodic_task(
        10.0,
        network_earnings_24h_to_redis.s(),
    )
    sender.add_periodic_task(
        10.0,
        network_earnings_6h_to_redis.s(),
    )
    sender.add_periodic_task(
        10.0,
        network_versions_to_redis.s(),
    )


app.conf.broker_url = 'redis://redis:6379/0'
app.conf.result_backend = 'redis://redis:6379/0'
