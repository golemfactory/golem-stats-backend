from django.db import models
from django.utils import timezone


class Node(models.Model):
    node_id = models.CharField(max_length=42, unique=True)
    wallet = models.CharField(max_length=42, null=True, blank=True)
    earnings_total = models.FloatField(null=True, blank=True)
    data = models.JSONField(null=True)
    online = models.BooleanField(default=False)
    computing_now = models.BooleanField(default=False)
    version = models.CharField(max_length=7)
    updated_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    hybrid = models.BooleanField(default=False)

    def save(self, *args, **kwargs):
        # If online is False, set computing_now to False
        if not self.online:
            self.computing_now = False
        super(Node, self).save(*args, **kwargs)


BENCHMARK_CHOICES = (
    ("primary", "primary"),
    ("secondary", "secondary"),
)


class Benchmark(models.Model):
    type = models.CharField(max_length=9, choices=BENCHMARK_CHOICES, default="primary")
    benchmark_score = models.IntegerField()
    benchmarked_at = models.DateTimeField(default=timezone.now)
    provider = models.ForeignKey(Node, on_delete=models.CASCADE)


class Network(models.Model):
    total_earnings = models.FloatField(null=True, blank=True)


class NetworkStatsMax(models.Model):
    online = models.IntegerField()
    cores = models.IntegerField()
    memory = models.FloatField()
    disk = models.FloatField()
    date = models.DateTimeField()
    runtime = models.CharField(max_length=64, default="vm")
    gpus = models.IntegerField(default=0)
    cuda_cores = models.IntegerField(default=0)
    gpu_memory = models.FloatField(default=0)


class NetworkStats(models.Model):
    online = models.IntegerField()
    cores = models.IntegerField()
    memory = models.FloatField()
    disk = models.FloatField()
    date = models.DateTimeField(auto_now=True)
    runtime = models.CharField(max_length=64, default="vm")
    gpus = models.IntegerField(default=0)
    cuda_cores = models.IntegerField(default=0)
    gpu_memory = models.FloatField(default=0)
    gpu_models = models.JSONField(default=dict)


class ProvidersComputing(models.Model):
    total = models.IntegerField()
    date = models.DateTimeField(auto_now=True)


class ProvidersComputingMax(models.Model):
    total = models.IntegerField()
    date = models.DateTimeField()


class NetworkMedianPricing(models.Model):
    start = models.FloatField()
    cpuh = models.FloatField()
    perh = models.FloatField()
    date = models.DateTimeField(auto_now=True)


class NetworkAveragePricing(models.Model):
    start = models.FloatField()
    cpuh = models.FloatField()
    perh = models.FloatField()
    date = models.DateTimeField(auto_now=True)


class NetworkMedianPricingMax(models.Model):
    start = models.FloatField()
    cpuh = models.FloatField()
    perh = models.FloatField()
    date = models.DateTimeField()


class NetworkAveragePricingMax(models.Model):
    start = models.FloatField()
    cpuh = models.FloatField()
    perh = models.FloatField()
    date = models.DateTimeField()


class Requestors(models.Model):
    node_id = models.CharField(max_length=42, unique=True)
    tasks_requested = models.FloatField(null=True, blank=True)


class requestor_scraper_check(models.Model):
    indexed_before = models.BooleanField(default=False)
