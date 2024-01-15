import subprocess
from django.conf import settings

from .models import Offer
def is_provider_online(provider):
    command = f"yagna net find {provider}"
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    is_online = "Exiting..., error details: Request failed" not in result.stderr
    return is_online


def identify_network(provider):
    # Use the variable from settings
    for driver in settings.GOLEM_MAINNET_PAYMENT_DRIVERS:
        # Fetch the related offers for the provider (Node)
        offers = Offer.objects.filter(provider=provider, runtime="vm")

        for offer in offers:
            vm_properties = offer.properties
            if vm_properties:
                # Check if any mainnet payment driver is present
                for driver in settings.GOLEM_MAINNET_PAYMENT_DRIVERS:
                    if f"golem.com.payment.platform.{driver}.address" in vm_properties:
                        return "mainnet"

    return "testnet"
