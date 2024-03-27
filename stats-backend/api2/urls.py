from django.urls import path
from django.shortcuts import render
from . import views

app_name = "api2"
from .views import (
    verify_provider_is_working,
    healthcheck_status,
    get_healthcheck_status,
)

urlpatterns = [
    path("network/online", views.network_online),
    path("network/online/donut", views.online_nodes_uptime_donut_data),
    path("network/online/new", views.network_online_new_stats_page),
    path("network/stats/online", views.online_nodes),
    path("network/stats/cpuvendor", views.cpu_vendor_stats),
    path("network/stats/cpuarchitecture", views.cpu_architecture_stats),
    path("network/online/stats", views.online_stats),
    path("network/online/runtime/stats", views.online_stats_by_runtime),
    path("network/online/flatmap", views.network_online_flatmap),
    path("network/offers/cheapest/cores", views.cheapest_by_cores),
    path("network/pricing/1h", views.pricing_past_hour),
    path("network/pricing/historical", views.historical_pricing_data),
    path("network/pricing/dump", views.task_pricing),
    path("provider/wallet/<wallet>", views.node_wallet),
    path("provider/node/<yagna_id>", views.node),
    path("provider/uptime/<yagna_id>", views.node_uptime),
    path("provider/earnings/<node_id>/<epoch>", views.get_transfer_sum),
    path("website/globe_data", views.globe_data),
    path("website/index", views.golem_main_website_index),
    path("network/historical/stats", views.network_historical_stats),
    path("network/comparison", views.list_ec2_instances_comparison),
    path("network/token/golemvschain", views.daily_volume_golem_vs_chain),
    path("network/transactions/volume", views.transaction_volume_over_time),
    path("network/amount/transfer", views.amount_transferred_over_time),
    path("initblockchain", views.init_golem_tx_manually),
    path("network/transactions/type/comparison", views.transaction_type_comparison),
    path("network/transactions/daily-type-counts", views.daily_transaction_type_counts),
    path(
        "network/transactions/average-value", views.average_transaction_value_over_time
    ),
    path("network/offers/cheapest", views.cheapest_offer),
    path(
        "healthcheck/start",
        verify_provider_is_working,
        name="verify_provider_is_workinge",
    ),
    path(
        "healthcheck/frontend/status",
        get_healthcheck_status,
        name="get_healthcheck_status",
    ),
    path("healthcheck/status", healthcheck_status, name="healthcheck_status"),
]
