from __future__ import absolute_import, unicode_literals
import os
from celery import Celery
import logging
from celery.schedules import crontab


logger = logging.getLogger("Celery")

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "core.settings")

app = Celery("core")


@app.on_after_finalize.connect
def setup_periodic_tasks(sender, **kwargs):
    from collector.tasks import (
        network_online_to_redis,
        network_stats_to_redis,
        network_utilization_to_redis,
        computing_now_to_redis,
        providers_average_earnings_to_redis,
        network_earnings,
        network_total_earnings,
        network_versions_to_redis,
        node_earnings_total,
        stats_snapshot_yesterday,
        network_median_pricing,
        network_average_pricing,
        computing_snapshot_yesterday,
        pricing_snapshot_yesterday,
        max_stats,
        networkstats_30m,
        network_node_versions,
        requestor_scraper,
        requestors_to_redis,
        market_agreement_termination_reasons,
        paid_invoices_1h,
        provider_accepted_invoices_1h,
        fetch_yagna_release,
        network_earnings_overview_new,
    )
    from api2.tasks import (
        v2_offer_scraper,
        v2_network_online_to_redis,
        v2_cheapest_provider,
        latest_blog_posts,
        v2_cheapest_offer,
        v2_network_online_to_redis_flatmap,
        get_current_glm_price,
        store_ec2_info,
        network_historical_stats_to_redis_v2,
        compare_ec2_and_golem,
        providers_who_received_tasks,
        create_pricing_snapshot,
        median_and_average_pricing_past_hour,
        chart_pricing_data_for_frontend,
        v2_network_online_to_redis_new_stats_page,
        online_nodes_uptime_donut_data,
        v2_network_stats_to_redis,
        sum_highest_runtime_resources,
        get_online_counts,
        count_cpu_vendors,
        count_cpu_architecture,
        online_nodes_computing,
        fetch_and_store_relay_nodes,
        init_golem_tx_scraping,
        fetch_latest_glm_tx,
        transaction_volume_over_time,
        amount_transferred_over_time,
        transaction_type_comparison,
        daily_transaction_type_counts,
        average_transaction_value_over_time,
        daily_volume_golem_vs_chain,
        computing_total_over_time,
        extract_wallets_and_ids,
    )

    sender.add_periodic_task(
        60,
        computing_total_over_time.s(),
        queue="default",
        options={"queue": "default", "routing_key": "default"},
    )
    sender.add_periodic_task(
        60,
        transaction_volume_over_time.s(),
        queue="default",
        options={"queue": "default", "routing_key": "default"},
    )
    sender.add_periodic_task(
        60,
        daily_volume_golem_vs_chain.s(),
        queue="default",
        options={"queue": "default", "routing_key": "default"},
    )
    sender.add_periodic_task(
        60,
        network_earnings_overview_new.s(),
        queue="default",
        options={"queue": "default", "routing_key": "default"},
    )
    sender.add_periodic_task(
        60,
        daily_transaction_type_counts.s(),
        queue="default",
        options={"queue": "default", "routing_key": "default"},
    )
    sender.add_periodic_task(
        60,
        average_transaction_value_over_time.s(),
        queue="default",
        options={"queue": "default", "routing_key": "default"},
    )
    sender.add_periodic_task(
        60,
        transaction_type_comparison.s(),
        queue="default",
        options={"queue": "default", "routing_key": "default"},
    )
    sender.add_periodic_task(
        60,
        amount_transferred_over_time.s(),
        queue="default",
        options={"queue": "default", "routing_key": "default"},
    )

    sender.add_periodic_task(
        crontab(hour="*/24"),
        store_ec2_info.s(),
        queue="default",
        options={"queue": "default", "routing_key": "default"},
    )
    sender.add_periodic_task(
        3600,
        init_golem_tx_scraping.s(),
        queue="default",
        options={"queue": "default", "routing_key": "default"},
    )
    sender.add_periodic_task(
        20,
        fetch_latest_glm_tx.s(),
        queue="default",
        options={"queue": "default", "routing_key": "default"},
    )
    sender.add_periodic_task(
        30,
        fetch_and_store_relay_nodes.s(),
        queue="default",
        options={"queue": "default", "routing_key": "default"},
    )
    sender.add_periodic_task(
        crontab(minute="*/60"),
        fetch_yagna_release.s(),
        queue="default",
        options={"queue": "default", "routing_key": "default"},
    )
    sender.add_periodic_task(
        60,
        compare_ec2_and_golem.s(),
        queue="default",
        options={"queue": "default", "routing_key": "default"},
    )
    sender.add_periodic_task(
        20,
        count_cpu_vendors.s(),
        queue="default",
        options={"queue": "default", "routing_key": "default"},
    )
    sender.add_periodic_task(
        20,
        count_cpu_architecture.s(),
        queue="default",
        options={"queue": "default", "routing_key": "default"},
    )
    sender.add_periodic_task(
        10,
        online_nodes_computing.s(),
        queue="default",
        options={"queue": "default", "routing_key": "default"},
    )
    sender.add_periodic_task(
        60,
        sum_highest_runtime_resources.s(),
        queue="default",
        options={"queue": "default", "routing_key": "default"},
    )
    sender.add_periodic_task(
        60,
        online_nodes_uptime_donut_data.s(),
        queue="default",
        options={"queue": "default", "routing_key": "default"},
    )

    sender.add_periodic_task(
        crontab(minute="*/10"),
        chart_pricing_data_for_frontend.s(),
        queue="default",
        options={"queue": "default", "routing_key": "default"},
    )
    sender.add_periodic_task(
        60,
        median_and_average_pricing_past_hour.s(),
        queue="default",
        options={"queue": "default", "routing_key": "default"},
    )
    sender.add_periodic_task(
        crontab(minute="*/10"),
        providers_who_received_tasks.s(),
        queue="default",
        options={"queue": "default", "routing_key": "default"},
    )

    sender.add_periodic_task(
        30.0,
        v2_cheapest_provider.s(),
        queue="default",
        options={"queue": "default", "routing_key": "default"},
    )
    sender.add_periodic_task(
        30.0,
        v2_cheapest_offer.s(),
        queue="default",
        options={"queue": "default", "routing_key": "default"},
    )
    sender.add_periodic_task(
        240.0,
        latest_blog_posts.s(),
        queue="default",
        options={"queue": "default", "routing_key": "default"},
    )
    sender.add_periodic_task(
        60.0,
        v2_offer_scraper.s(),
        queue="yagna",
        options={"queue": "yagna", "routing_key": "yagna"},
    )
    sender.add_periodic_task(
        60.0,
        v2_offer_scraper.s(subnet_tag="ray-on-golem-heads"),
        queue="yagna",
        options={"queue": "yagna", "routing_key": "yagna"},
    )
    sender.add_periodic_task(
        20.0,
        v2_network_online_to_redis.s(),
        queue="default",
        options={"queue": "default", "routing_key": "default"},
    )
    sender.add_periodic_task(
        30.0,
        get_online_counts.s(),
        queue="default",
        options={"queue": "default", "routing_key": "default"},
    )
    # Disabled as not in use currently
    # sender.add_periodic_task(
    #     20.0,
    #     v2_network_online_to_redis_new_stats_page.s(runtime="vm"),
    #     queue="default",
    #     options={"queue": "default", "routing_key": "default"},
    # )
    # Disabled as not in use currently
    # sender.add_periodic_task(
    #     20.0,
    #     v2_network_online_to_redis_new_stats_page.s(runtime="vm-nvidia"),
    #     queue="default",
    #     options={"queue": "default", "routing_key": "default"},
    # )
    # Disabled as not in use currently
    # sender.add_periodic_task(
    #     20.0,
    #     v2_network_online_to_redis_new_stats_page.s(),
    #     queue="default",
    #     options={"queue": "default", "routing_key": "default"},
    # )
    # sender.add_periodic_task(
    #     20.0,
    #     v2_network_online_to_redis_flatmap.s(),
    #     queue="default",
    #     options={"queue": "default", "routing_key": "default"},
    # )

    # sender.add_periodic_task(
    #     10.0,
    #     requestor_scraper.s(),
    #     queue="default",
    #     options={"queue": "default", "routing_key": "default"},
    # )
    sender.add_periodic_task(
        60.0,
        get_current_glm_price.s(),
        queue="default",
        options={"queue": "default", "routing_key": "default"},
    )
    sender.add_periodic_task(
        crontab(hour=0, minute=1),  # 00:01
        create_pricing_snapshot.s(network="mainnet"),
        queue="default",
        options={"queue": "default", "routing_key": "default"},
    )
    sender.add_periodic_task(
        crontab(hour=0, minute=1),  # 00:01
        create_pricing_snapshot.s(network="testnet"),
        queue="default",
        options={"queue": "default", "routing_key": "default"},
    )

    sender.add_periodic_task(
        60,
        networkstats_30m.s(),
        queue="default",
        options={"queue": "default", "routing_key": "default"},
    )
    sender.add_periodic_task(
        60,
        network_historical_stats_to_redis_v2.s(),
        queue="default",
        options={"queue": "default", "routing_key": "default"},
    )

    sender.add_periodic_task(
        15.0,
        network_median_pricing.s(),
        queue="default",
        options={"queue": "default", "routing_key": "default"},
    )
    sender.add_periodic_task(
        15.0,
        paid_invoices_1h.s(),
        queue="default",
        options={"queue": "default", "routing_key": "default"},
    )
    sender.add_periodic_task(
        15.0,
        provider_accepted_invoices_1h.s(),
        queue="default",
        options={"queue": "default", "routing_key": "default"},
    )
    sender.add_periodic_task(
        15.0,
        market_agreement_termination_reasons.s(),
        queue="default",
        options={"queue": "default", "routing_key": "default"},
    )
    sender.add_periodic_task(
        15.0,
        network_average_pricing.s(),
        queue="default",
        options={"queue": "default", "routing_key": "default"},
    )
    sender.add_periodic_task(
        crontab(minute=0, hour=0),
        stats_snapshot_yesterday.s(),
        queue="default",
        options={"queue": "default", "routing_key": "default"},
    )
    sender.add_periodic_task(
        crontab(minute=0, hour=0),
        pricing_snapshot_yesterday.s(),
        queue="default",
        options={"queue": "default", "routing_key": "default"},
    )
    sender.add_periodic_task(
        crontab(minute=0, hour=0),
        computing_snapshot_yesterday.s(),
        queue="default",
        options={"queue": "default", "routing_key": "default"},
    )
    sender.add_periodic_task(
        crontab(minute="*/10"),
        node_earnings_total.s(node_version="v1"),
        queue="default",
        options={"queue": "default", "routing_key": "default"},
    )
    sender.add_periodic_task(
        crontab(minute="*/10"),
        node_earnings_total.s(node_version="v2"),
        queue="default",
        options={"queue": "default", "routing_key": "default"},
    )
    sender.add_periodic_task(
        crontab(minute="*/1"),
        network_node_versions.s(),
        queue="default",
        options={"queue": "default", "routing_key": "default"},
    )
    sender.add_periodic_task(
        crontab(hour="*/1"),
        max_stats.s(),
        queue="default",
        options={"queue": "default", "routing_key": "default"},
    )
    # sender.add_periodic_task(
    #     10.0,
    #     network_online_to_redis.s(),
    #     queue="default",
    #     options={"queue": "default", "routing_key": "default"},
    # )
    sender.add_periodic_task(
        10.0,
        network_stats_to_redis.s(),
        queue="default",
        options={"queue": "default", "routing_key": "default"},
    )
    sender.add_periodic_task(
        10.0,
        v2_network_stats_to_redis.s(),
        queue="default",
        options={"queue": "default", "routing_key": "default"},
    )
    sender.add_periodic_task(
        10.0,
        network_utilization_to_redis.s(),
        queue="default",
        options={"queue": "default", "routing_key": "default"},
    )
    sender.add_periodic_task(
        10.0,
        requestors_to_redis.s(),
        queue="default",
        options={"queue": "default", "routing_key": "default"},
    )
    sender.add_periodic_task(
        10.0,
        computing_now_to_redis.s(),
        queue="default",
        options={"queue": "default", "routing_key": "default"},
    )
    sender.add_periodic_task(
        10.0,
        providers_average_earnings_to_redis.s(),
        queue="default",
        options={"queue": "default", "routing_key": "default"},
    )
    sender.add_periodic_task(
        30.0,
        network_earnings.s(hours="6h"),
        queue="default",
        options={"queue": "default", "routing_key": "default"},
    )
    sender.add_periodic_task(
        30.0,
        # 90 days
        network_earnings.s(hours="2160h"),
        queue="default",
        options={"queue": "default", "routing_key": "default"},
    )
    sender.add_periodic_task(
        30.0,
        # 30 days
        network_earnings.s(hours="720h"),
        queue="default",
        options={"queue": "default", "routing_key": "default"},
    )
    sender.add_periodic_task(
        30.0,
        # 7 days
        network_earnings.s(hours="168h"),
        queue="default",
        options={"queue": "default", "routing_key": "default"},
    )
    sender.add_periodic_task(
        110,
        network_total_earnings.s(),
        queue="default",
        options={"queue": "default", "routing_key": "default"},
    )
    sender.add_periodic_task(
        30.0,
        network_earnings.s(hours="24h"),
        queue="default",
        options={"queue": "default", "routing_key": "default"},
    )
    sender.add_periodic_task(
        30.0,
        network_versions_to_redis.s(),
        queue="default",
        options={"queue": "default", "routing_key": "default"},
    )
    sender.add_periodic_task(
        30,
        extract_wallets_and_ids.s(),
        queue="default",
        options={"queue": "default", "routing_key": "default"},
    )


app.conf.task_default_queue = "default"
app.conf.broker_url = "redis://redis:6379/0"
app.conf.result_backend = "redis://redis:6379/0"
app.conf.task_routes = {
    "app.tasks.default": {"queue": "default"},
    "app.tasks.yagna": {"queue": "yagna"},
}
app.conf.broker_connection_retry_on_startup = True
