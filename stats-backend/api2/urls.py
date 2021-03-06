from django.urls import path
from django.shortcuts import render
from . import views

app_name = 'api'

urlpatterns = [
    path('network/online', views.network_online),
    path('node/<yagna_id>', views.node),
    path('website/globe_data', views.globe_data),
    path('website/index', views.golem_main_website_index),
]
