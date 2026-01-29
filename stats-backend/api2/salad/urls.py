from django.urls import path

from . import views

app_name = "salad"

urlpatterns = [
    path("network/stats", views.current_stats, name="current_stats"),
    path("network/online/stats", views.online_stats, name="online_stats"),
    path("network/computing", views.computing_now, name="computing_now"),
    path("network/versions", views.network_versions, name="network_versions"),
    path("network/utilization", views.network_utilization, name="network_utilization"),
    path(
        "network/earnings/overview",
        views.network_earnings_overview,
        name="earnings_overview",
    ),
    path("network/historical/stats", views.historical_stats, name="historical_stats"),
    path(
        "network/historical/computing",
        views.computing_daily,
        name="computing_daily",
    ),
]
