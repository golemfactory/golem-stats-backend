from django.urls import path
from django.shortcuts import render
from . import views

app_name = 'api'

urlpatterns = [
    path('network/<int:start>/<int:end>', views.network_utilization),
    path('provider/computing', views.providers_computing_currently),
    path('provider/node/<yagna_id>', views.node),
    path('provider/node/<yagna_id>/computing', views.provider_computing),
    path('provider/node/<yagna_id>/earnings/<int:hours>',
         views.payments_last_n_hours_provider),
    path('provider/node/<yagna_id>/activity', views.activity_graph_provider),
    path('provider/average/earnings', views.providers_average_earnings),
    path('network/earnings/6', views.network_earnings_6h),
    path('network/earnings/24', views.network_earnings_24h),
    path('network/online', views.online_nodes),
    path('network/online/stats', views.general_stats),
    path('network/versions', views.network_versions),
    path('wallet/<wallet>', views.node_wallet),
]
