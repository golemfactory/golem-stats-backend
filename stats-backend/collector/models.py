from django.db import models
from django.contrib.postgres.fields import JSONField

# Create your models here.

class Node(models.Model):
    name = models.CharField(max_length=42)
    data = JSONField()
 
    def __str__(self):
        return self.name