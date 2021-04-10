from django.urls import path
from django.shortcuts import render
from . import views

app_name = 'api'

urlpatterns = [
    path('network/<int:start>/<int:end>', views.network_utilization),
    path('provider/computing', views.providers_computing_currently),
    path('provider/average/earnings', views.providers_average_earnings),
    path('network/earnings/<int:hours>', views.network_earnings),
    path('network/online', views.online_nodes),
    path('network/online/stats', views.general_stats),
]
