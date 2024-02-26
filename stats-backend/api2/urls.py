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
    path("network/online/flatmap", views.network_online_flatmap),
    path("network/offers/cheapest/cores", views.cheapest_by_cores),
    path("network/pricing/median/1h", views.get_median_pricing_1h),
    path("provider/wallet/<wallet>", views.node_wallet),
    path("provider/node/<yagna_id>", views.node),
    path("provider/uptime/<yagna_id>", views.node_uptime),
    path("website/globe_data", views.globe_data),
    path("website/index", views.golem_main_website_index),
    path("network/historical/stats", views.network_historical_stats),
    path("network/comparison", views.list_ec2_instances_comparison),
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
