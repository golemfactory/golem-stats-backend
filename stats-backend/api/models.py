from django.db import models
from django.utils import timezone


# Create your models here.


class APICounter(models.Model):
    endpoint = models.CharField(max_length=50)
