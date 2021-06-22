from django.contrib import admin
from .models import Network


@admin.register(Network)
class networkadmin(admin.ModelAdmin):
    list_display = ("total_earnings", )
