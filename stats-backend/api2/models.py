from django.db import models
from django.utils import timezone
from django.db.models.functions import Extract, Coalesce, Lag
from metamask.models import UserProfile

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
    uptime_created_at = models.DateTimeField(auto_now_add=True)


class EC2Instance(models.Model):
    name = models.CharField(max_length=100, unique=True)
    vcpu = models.IntegerField(null=True)
    memory = models.FloatField(null=True)  # Assuming memory is in GB
    price_usd = models.DecimalField(max_digits=10, decimal_places=2, null=True)

    def __str__(self):
        return self.name


class Offer(models.Model):
    properties = models.JSONField(null=True)
    runtime = models.CharField(max_length=42)
    provider = models.ForeignKey(Node, on_delete=models.CASCADE)
    updated_at = models.DateTimeField(auto_now=True)
    created_at = models.DateTimeField(auto_now_add=True)
    monthly_price_glm = models.FloatField(null=True, blank=True)
    monthly_price_usd = models.FloatField(null=True, blank=True)
    is_overpriced = models.BooleanField(default=False)
    overpriced_compared_to = models.ForeignKey(
        EC2Instance, on_delete=models.CASCADE, null=True
    )
    suggest_env_per_hour_price = models.FloatField(null=True)
    times_more_expensive = models.FloatField(null=True)
    cheaper_than = models.ForeignKey(
        EC2Instance, on_delete=models.CASCADE, null=True, related_name="cheaper_offers"
    )
    times_cheaper = models.FloatField(null=True)

    class Meta:
        unique_together = (
            "runtime",
            "provider",
        )


class HealtcheckTask(models.Model):
    provider = models.ForeignKey(Node, on_delete=models.CASCADE)
    user = models.ForeignKey(UserProfile, on_delete=models.CASCADE)
    status = models.TextField()
    updated_at = models.DateTimeField(auto_now=True)
    created_at = models.DateTimeField(auto_now_add=True)


class GLM(models.Model):
    current_price = models.FloatField(null=True)


class NodeStatusHistory(models.Model):
    provider = models.ForeignKey(Node, on_delete=models.CASCADE)
    is_online = models.BooleanField()
    timestamp = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.provider.node_id} - {'Online' if self.is_online else 'Offline'} at {self.timestamp}"
