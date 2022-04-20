from django.db import models
from django.utils import timezone


# Create your models here.


class Node(models.Model):
    node_id = models.CharField(max_length=42, unique=True)
    wallet = models.CharField(max_length=42, null=True, blank=True)
    earnings_total = models.FloatField(null=True, blank=True)
    online = models.BooleanField(default=False)
    computing_now = models.BooleanField(default=False)
    version = models.CharField(max_length=7)
    updated_at = models.DateTimeField(auto_now=True)
    created_at = models.DateTimeField(auto_now_add=True)


class Offer(models.Model):
    properties = models.JSONField(null=True)
    runtime = models.CharField(max_length=42)
    provider = models.ForeignKey(Node, on_delete=models.CASCADE)
    updated_at = models.DateTimeField(auto_now=True)
    created_at = models.DateTimeField(auto_now_add=True)
    monthly_price_glm = models.FloatField(null=True, blank=True)

    class Meta:
        unique_together = ('runtime', 'provider',)
