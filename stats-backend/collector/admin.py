from django.contrib import admin
from .models import Network, Requestors


@admin.register(Network)
class networkadmin(admin.ModelAdmin):
    list_display = ("total_earnings", )


@admin.register(Requestors)
class requestoradmin(admin.ModelAdmin):
    list_display = ("node_id", "tasks_requested")
