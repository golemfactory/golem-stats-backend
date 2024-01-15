from django.urls import path
from . import views

urlpatterns = [
    path('user/find', views.find_user_by_wallet_address),
    path('user/create', views.create_user_on_backend),
    path('user/verify', views.verify_wallet_signature),
    path('user/refresh', views.refresh_token),
]
