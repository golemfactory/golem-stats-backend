from django.db import models
from django.utils import timezone


# Create your models here.


class Node(models.Model):
    node_id = models.CharField(max_length=42, unique=True)
    wallet = models.CharField(max_length=42, null=True, blank=True)
    earnings_total = models.FloatField(null=True, blank=True)
    data = models.JSONField(null=True)
    online = models.BooleanField(default=False)
    updated_at = models.DateTimeField(null=True, blank=True)


class NetworkStatsMax(models.Model):
    online = models.IntegerField()
    cores = models.IntegerField()
    memory = models.FloatField()
    disk = models.FloatField()
    date = models.DateTimeField()


class NetworkStats(models.Model):
    online = models.IntegerField()
    cores = models.IntegerField()
    memory = models.FloatField()
    disk = models.FloatField()
    date = models.DateTimeField(auto_now=True)


class ProvidersComputing(models.Model):
    total = models.IntegerField()
