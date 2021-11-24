from django.db import models
from django.db.models.fields import BigIntegerField, CharField
from django.utils import timezone


# Create your models here.


class APICounter(models.Model):
    endpoint = CharField(max_length=50)


class APIHits(models.Model):
    count = BigIntegerField()
