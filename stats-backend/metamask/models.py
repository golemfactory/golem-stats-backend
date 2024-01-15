from django.contrib.auth.models import AbstractUser
from django.db import models
from .utils import generate_nonce

class User(AbstractUser):
    wallet_address = models.CharField(max_length=42, unique=True)
    web3_nonce = models.CharField(max_length=255, default='')

    class Meta:
        verbose_name = 'MetaMask User'
        verbose_name_plural = 'MetaMask Users'

    # Overriding the groups and user_permissions fields to set a unique related_name
    groups = models.ManyToManyField(
        'auth.Group',
        verbose_name='groups',
        blank=True,
        related_name="metamask_user_set",  # Unique related_name
        related_query_name="metamask_user",
    )
    user_permissions = models.ManyToManyField(
        'auth.Permission',
        verbose_name='user permissions',
        blank=True,
        related_name="metamask_user_set",  # Unique related_name
        related_query_name="metamask_user",
    )
    def update_nonce(self):
        self.web3_nonce = generate_nonce()  # Assuming generate_nonce() is defined and imported
        self.save()
