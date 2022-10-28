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
    from collector.tasks import offer_scraper, v1_offer_scraper_hybrid, online_nodes_computing, network_online_to_redis, network_stats_to_redis, network_utilization_to_redis, computing_now_to_redis, providers_average_earnings_to_redis, network_earnings_6h_to_redis, network_earnings_24h_to_redis, network_total_earnings, network_versions_to_redis, node_earnings_total, stats_snapshot_yesterday, requests_served, network_median_pricing, network_average_pricing, computing_snapshot_yesterday, pricing_snapshot_yesterday, max_stats, networkstats_30m, network_node_versions, requestor_scraper, requestors_to_redis, market_agreement_termination_reasons, paid_invoices_1h, provider_accepted_invoices_1h, save_endpoint_logs_to_db
    from api2.tasks import v2_offer_scraper, v2_network_online_to_redis, v2_cheapest_provider, latest_blog_posts, v2_cheapest_offer
    sender.add_periodic_task(
        30.0,
        offer_scraper.s(),
        queue='yagna',
        options={
            'queue': 'yagna',
            'routing_key': 'yagna'}
    )
    sender.add_periodic_task(
        30.0,
        v2_cheapest_provider.s(),
        queue='default',
        options={
            'queue': 'default',
            'routing_key': 'default'}
    )
    sender.add_periodic_task(
        30.0,
        v2_cheapest_offer.s(),
        queue='default',
        options={
            'queue': 'default',
            'routing_key': 'default'}
    )
    sender.add_periodic_task(
        30.0,
        latest_blog_posts.s(),
        queue='default',
        options={
            'queue': 'default',
            'routing_key': 'default'}
    )
    sender.add_periodic_task(
        30.0,
        v2_offer_scraper.s(),
        queue='yagna',
        options={
            'queue': 'yagna',
            'routing_key': 'yagna'}
    )
    sender.add_periodic_task(
        30.0,
        v1_offer_scraper_hybrid.s(),
        queue='yagna-hybrid',
        options={
            'queue': 'yagna-hybrid',
            'routing_key': 'yagnahybrid'}
    )
    sender.add_periodic_task(
        10.0,
        v2_network_online_to_redis.s(),
        queue='default',
        options={
            'queue': 'default',
            'routing_key': 'default'}
    )
    sender.add_periodic_task(
        30.0,
        online_nodes_computing.s(),
        queue='default',
        options={
            'queue': 'default',
            'routing_key': 'default'}
    )
    sender.add_periodic_task(
        10.0,
        requestor_scraper.s(),
        queue='default',
        options={
            'queue': 'default',
            'routing_key': 'default'}
    )
    # sender.add_periodic_task(
    #     10.0,
    #     save_endpoint_logs_to_db.s(),
    #     queue='default',
    #     options={
    #         'queue': 'default',
    #         'routing_key': 'default'}
    # )
    sender.add_periodic_task(
        60,
        networkstats_30m.s(),
        queue='default',
        options={
            'queue': 'default',
            'routing_key': 'default'}
    )
    # sender.add_periodic_task(
    #     10.0,
    #     requests_served.s(),
    #     queue='default',
    #     options={
    #         'queue': 'default',
    #         'routing_key': 'default'}
    # )
    sender.add_periodic_task(
        15.0,
        network_median_pricing.s(),
        queue='default',
        options={
            'queue': 'default',
            'routing_key': 'default'}
    )
    sender.add_periodic_task(
        15.0,
        paid_invoices_1h.s(),
        queue='default',
        options={
            'queue': 'default',
            'routing_key': 'default'}
    )
    sender.add_periodic_task(
        15.0,
        provider_accepted_invoices_1h.s(),
        queue='default',
        options={
            'queue': 'default',
            'routing_key': 'default'}
    )
    sender.add_periodic_task(
        15.0,
        market_agreement_termination_reasons.s(),
        queue='default',
        options={
            'queue': 'default',
            'routing_key': 'default'}
    )
    sender.add_periodic_task(
        15.0,
        network_average_pricing.s(),
        queue='default',
        options={
            'queue': 'default',
            'routing_key': 'default'}
    )
    sender.add_periodic_task(
        crontab(minute=0, hour=0),
        stats_snapshot_yesterday.s(),
        queue='default',
        options={
            'queue': 'default',
            'routing_key': 'default'}
    )
    sender.add_periodic_task(
        crontab(minute=0, hour=0),
        pricing_snapshot_yesterday.s(),
        queue='default',
        options={
            'queue': 'default',
            'routing_key': 'default'}
    )
    sender.add_periodic_task(
        crontab(minute=0, hour=0),
        computing_snapshot_yesterday.s(),
        queue='default',
        options={
            'queue': 'default',
            'routing_key': 'default'}
    )
    sender.add_periodic_task(
        crontab(minute="*/10"),
        node_earnings_total.s(node_version='v1'),
        queue='default',
        options={
            'queue': 'default',
            'routing_key': 'default'}
    )
    sender.add_periodic_task(
        crontab(minute="*/1"),
        node_earnings_total.s(node_version='v2'),
        queue='default',
        options={
            'queue': 'default',
            'routing_key': 'default'}
    )
    sender.add_periodic_task(
        crontab(minute="*/1"),
        network_node_versions.s(),
        queue='default',
        options={
            'queue': 'default',
            'routing_key': 'default'}
    )
    sender.add_periodic_task(
        crontab(hour="*/1"),
        max_stats.s(),
        queue='default',
        options={
            'queue': 'default',
            'routing_key': 'default'}
    )
    sender.add_periodic_task(
        10.0,
        network_online_to_redis.s(),
        queue='default',
        options={
            'queue': 'default',
            'routing_key': 'default'}
    )
    sender.add_periodic_task(
        10.0,
        network_stats_to_redis.s(),
        queue='default',
        options={
            'queue': 'default',
            'routing_key': 'default'}
    )
    sender.add_periodic_task(
        10.0,
        network_utilization_to_redis.s(),
        queue='default',
        options={
            'queue': 'default',
            'routing_key': 'default'}
    )
    sender.add_periodic_task(
        10.0,
        requestors_to_redis.s(),
        queue='default',
        options={
            'queue': 'default',
            'routing_key': 'default'}
    )
    sender.add_periodic_task(
        10.0,
        computing_now_to_redis.s(),
        queue='default',
        options={
            'queue': 'default',
            'routing_key': 'default'}
    )
    sender.add_periodic_task(
        10.0,
        providers_average_earnings_to_redis.s(),
        queue='default',
        options={
            'queue': 'default',
            'routing_key': 'default'}
    )
    sender.add_periodic_task(
        10.0,
        network_earnings_24h_to_redis.s(),
        queue='default',
        options={
            'queue': 'default',
            'routing_key': 'default'}
    )
    sender.add_periodic_task(
        crontab(minute="*/1"),
        network_total_earnings.s(),
        queue='default',
        options={
            'queue': 'default',
            'routing_key': 'default'}
    )
    sender.add_periodic_task(
        10.0,
        network_earnings_6h_to_redis.s(),
        queue='default',
        options={
            'queue': 'default',
            'routing_key': 'default'}
    )
    sender.add_periodic_task(
        10.0,
        network_versions_to_redis.s(),
        queue='default',
        options={
            'queue': 'default',
            'routing_key': 'default'})


app.conf.task_default_queue = 'default'
app.conf.broker_url = 'redis://redis:6379/0'
app.conf.result_backend = 'redis://redis:6379/0'
app.conf.task_routes = {'app.tasks.default': {
    'queue': 'default'}, 'app.tasks.yagna': {'queue': 'yagna'}, 'app.tasks.yagnahybrid': {'queue': 'yagna-hybrid'}}
