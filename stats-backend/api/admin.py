from django.contrib import admin
from .models import APIHits


@admin.register(APIHits)
class apihitsadmin(admin.ModelAdmin):
    list_display = ("count", )
