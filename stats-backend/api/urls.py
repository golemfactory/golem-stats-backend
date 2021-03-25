from django.urls import path

from . import views

app_name = 'api'

urlpatterns = [
    path('network/<int:start>/<int:end>', views.network_utilization),
    path('provider/computing', views.providers_computing_currently),
    path('network/earnings/<int:hours>', views.network_earnings),
]
