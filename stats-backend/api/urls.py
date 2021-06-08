from django.urls import path
from django.shortcuts import render
from . import views

app_name = 'api'

urlpatterns = [
    path('provider/computing', views.providers_computing_currently),
    path('provider/node/<yagna_id>', views.node),
    path('provider/node/<yagna_id>/computing', views.provider_computing),
    path('provider/node/<yagna_id>/earnings/<int:hours>',
         views.payments_last_n_hours_provider),
    path('provider/node/<yagna_id>/activity', views.activity_graph_provider),
    path('provider/node/<yagna_id>/total/computed', views.total_tasks_computed),
    path('provider/node/<yagna_id>/total/computed/seconds',
         views.provider_seconds_computed_total),
    path('provider/average/earnings', views.providers_average_earnings),
    path('provider/wallet/<wallet>', views.node_wallet),
    path('network/earnings/6', views.network_earnings_6h),
    path('network/earnings/24', views.network_earnings_24h),
    path('network/earnings/365d', views.network_earnings_365d),
    path('network/online', views.online_nodes),
    path('network/online/stats', views.general_stats),
    path('network/<int:start>/<int:end>', views.network_utilization),
    path('network/versions', views.network_versions),
    path('network/historical/stats', views.statsmax),
    path('network/historical/stats/computing', views.computing_total),
    path('network/historical/stats/6h', views.stats_6h),
    path('network/historical/pricing/median', views.medianpricingmax),
    path('network/historical/pricing/average', views.avgpricingmax),
    path('network/historical/provider/computing', views.providercomputingmax),
    path('network/historical/nodes/<int:number>', views.latest_nodes_n),
    path('network/historical/nodes', views.latest_nodes),
    path('network/pricing/median', views.median_prices),
    path('network/pricing/average', views.average_pricing),
    path('api/usage', views.total_api_calls),
]
