from django.db import models
from django.contrib.auth.models import User
from .utils import generate_nonce


class UserProfile(models.Model):
    user = models.OneToOneField(User, on_delete=models.CASCADE)
    wallet_address = models.CharField(max_length=42, unique=True)
    web3_nonce = models.CharField(max_length=255, default="")

    def update_nonce(self):
        self.web3_nonce = generate_nonce()
        self.save()
