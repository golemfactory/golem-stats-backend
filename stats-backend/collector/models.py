from django.db import models

# Create your models here.


class Node(models.Model):
    node_id = models.CharField(max_length=42, unique=True)
    data = models.JSONField(null=True)
    online = models.BooleanField(default=False)
