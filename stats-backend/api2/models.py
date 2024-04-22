from django.db import models
from django.utils import timezone
from django.db.models.functions import Extract, Coalesce, Lag
from metamask.models import UserProfile

# Create your models here.


class Node(models.Model):
    node_id = models.CharField(max_length=42, unique=True, db_index=True)
    wallet = models.CharField(max_length=42, null=True, blank=True, db_index=True)
    online = models.BooleanField(default=False, db_index=True)
    earnings_total = models.FloatField(null=True, blank=True)
    computing_now = models.BooleanField(default=False, db_index=True)
    version = models.CharField(max_length=7, db_index=True)
    updated_at = models.DateTimeField(auto_now=True, db_index=True)
    created_at = models.DateTimeField(auto_now_add=True, db_index=True)
    uptime_created_at = models.DateTimeField(auto_now_add=True, db_index=True)
    network = models.CharField(max_length=42, default="mainnet", db_index=True)

    def save(self, *args, **kwargs):
        if not self.online:
            self.computing_now = False
        super(Node, self).save(*args, **kwargs)

    class Meta:
        indexes = [
            models.Index(fields=["online", "computing_now"]),
            models.Index(fields=["network", "online"]),
        ]


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
    hourly_price_glm = models.FloatField(null=True, blank=True)
    hourly_price_usd = models.FloatField(null=True, blank=True)
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
        unique_together = ("runtime", "provider")
        indexes = [
            models.Index(fields=["provider", "runtime"]),
            models.Index(fields=["is_overpriced", "overpriced_compared_to"]),
            models.Index(fields=["cheaper_than"]),
        ]


class HealtcheckTask(models.Model):
    provider = models.ForeignKey(Node, on_delete=models.CASCADE)
    user = models.ForeignKey(UserProfile, on_delete=models.CASCADE)
    status = models.TextField()
    updated_at = models.DateTimeField(auto_now=True)
    created_at = models.DateTimeField(auto_now_add=True)


class GLM(models.Model):
    current_price = models.FloatField(null=True)


class NodeStatusHistory(models.Model):
    provider = models.ForeignKey(Node, on_delete=models.CASCADE, db_index=True)
    is_online = models.BooleanField(db_index=True)
    timestamp = models.DateTimeField(auto_now_add=True, db_index=True)

    def __str__(self):
        return f"{self.provider.node_id} - {'Online' if self.is_online else 'Offline'} at {self.timestamp}"

    class Meta:
        indexes = [
            models.Index(fields=["provider", "timestamp"]),
        ]


class ProviderWithTask(models.Model):
    instance = models.ForeignKey(
        Node, on_delete=models.CASCADE, related_name="tasks_received"
    )
    offer = models.ForeignKey(Offer, on_delete=models.CASCADE)
    cpu_per_hour = models.FloatField(
        null=True, blank=True
    )  # Pricing per second for cpu/h
    env_per_hour = models.FloatField(
        null=True, blank=True
    )  # Pricing per second for duration/env/h
    start_price = models.FloatField(null=True, blank=True)  # Static start price
    created_at = models.DateTimeField(auto_now_add=True)
    network = models.CharField(max_length=42, default="mainnet")


class PricingSnapshot(models.Model):
    average_cpu_price = models.FloatField(default=0)
    median_cpu_price = models.FloatField(default=0)
    average_env_price = models.FloatField(default=0)
    median_env_price = models.FloatField(default=0)
    average_start_price = models.FloatField(default=0)
    median_start_price = models.FloatField(default=0)
    created_at = models.DateTimeField(auto_now_add=True)
    date = models.DateTimeField(null=True, blank=True)
    network = models.CharField(max_length=42, default="mainnet")


class RelayNodes(models.Model):
    node_id = models.CharField(max_length=42, unique=True)


class GolemTransactions(models.Model):
    scanner_id = models.IntegerField(primary_key=True)
    txhash = models.CharField(max_length=66, db_index=True)
    transaction_type = models.CharField(
        max_length=42, null=True, blank=True, db_index=True
    )
    amount = models.FloatField()
    timestamp = models.DateTimeField(db_index=True)
    receiver = models.CharField(max_length=42, db_index=True)
    sender = models.CharField(max_length=42, db_index=True)
    tx_from_golem = models.BooleanField(default=False, db_index=True)

    class Meta:
        indexes = [
            models.Index(fields=["txhash"]),
            models.Index(fields=["transaction_type"]),
            models.Index(fields=["timestamp"]),
            models.Index(
                fields=["receiver", "sender"]
            ),  # Compound index example if you often filter by both receiver and sender
        ]


class TransactionScraperIndex(models.Model):
    indexed_before = models.BooleanField(default=False)
    latest_timestamp_indexed = models.DateTimeField(null=True, blank=True)
