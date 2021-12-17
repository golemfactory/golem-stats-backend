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
    from collector.tasks import offer_scraper, network_online_to_redis, network_stats_to_redis, network_utilization_to_redis, computing_now_to_redis, providers_average_earnings_to_redis, network_earnings_6h_to_redis, network_earnings_24h_to_redis, network_total_earnings, network_versions_to_redis, node_earnings_total, stats_snapshot_yesterday, requests_served, network_median_pricing, network_average_pricing, computing_snapshot_yesterday, pricing_snapshot_yesterday, max_stats, networkstats_30m, network_node_versions, requestor_scraper, requestors_to_redis, market_agreement_termination_reasons, paid_invoices_1h, provider_accepted_invoices_1h, save_endpoint_logs_to_db
    sender.add_periodic_task(
        30.0,
        offer_scraper.s(),
    )
    sender.add_periodic_task(
        10.0,
        requestor_scraper.s(),
    )
    # sender.add_periodic_task(
    #     10.0,
    #     save_endpoint_logs_to_db.s(),
    # )
    sender.add_periodic_task(
        60,
        networkstats_30m.s(),
    )
    # sender.add_periodic_task(
    #     10.0,
    #     requests_served.s(),
    # )
    sender.add_periodic_task(
        15.0,
        network_median_pricing.s(),
    )
    sender.add_periodic_task(
        15.0,
        paid_invoices_1h.s(),
    )
    sender.add_periodic_task(
        15.0,
        provider_accepted_invoices_1h.s(),
    )
    sender.add_periodic_task(
        15.0,
        market_agreement_termination_reasons.s(),
    )
    sender.add_periodic_task(
        15.0,
        network_average_pricing.s(),
    )
    sender.add_periodic_task(
        crontab(minute=0, hour=0),
        stats_snapshot_yesterday.s(),
    )
    sender.add_periodic_task(
        crontab(minute=0, hour=0),
        pricing_snapshot_yesterday.s(),
    )
    sender.add_periodic_task(
        crontab(minute=0, hour=0),
        computing_snapshot_yesterday.s(),
    )
    sender.add_periodic_task(
        crontab(minute="*/10"),
        node_earnings_total.s(),
    )
    sender.add_periodic_task(
        crontab(minute="*/1"),
        network_node_versions.s(),
    )
    sender.add_periodic_task(
        crontab(hour="*/1"),
        max_stats.s(),
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
        requestors_to_redis.s(),
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
        crontab(minute="*/1"),
        network_total_earnings.s(),
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
