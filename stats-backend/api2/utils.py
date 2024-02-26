import subprocess
from django.conf import settings
import os
from .models import Offer


def identify_network_by_offer(offer):
    for driver in settings.GOLEM_MAINNET_PAYMENT_DRIVERS:
        if f"golem.com.payment.platform.{driver}.address" in offer.properties:
            return "mainnet"
    return "testnet"


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


import requests
import time


def make_request_with_rate_limit_handling(url, headers):
    while True:
        response = requests.get(url, headers=headers)
        if response.status_code == 429:  # Rate limit hit

            reset_time = int(response.headers.get("x-rate-limit-reset", 0))
            sleep_duration = max(reset_time - time.time(), 0)
            print(f"Ratelimited waiting for {sleep_duration}")
            time.sleep(sleep_duration + 1)  # Sleep until the limit resets, then retry
        else:
            return response


def get_ec2_products():
    products = []
    url = "https://api.vantage.sh/v2/products?service_id=aws-ec2"
    headers = {
        "accept": "application/json",
        "authorization": f'Bearer {os.environ.get("VANTAGE_API_KEY")}',
    }

    while url:
        response = make_request_with_rate_limit_handling(url, headers)
        data = response.json()
        print("Got product list")
        products.extend(data.get("products", []))
        url = data["links"].get("next")  # Get the next page URL

        return products


def get_pricing(product_id):
    url = f"https://api.vantage.sh/v2/products/{product_id}/prices"
    headers = {
        "accept": "application/json",
        "authorization": f'Bearer {os.environ.get("VANTAGE_API_KEY")}',
    }
    response = make_request_with_rate_limit_handling(url, headers)
    print("Got price")
    return response.json()


def find_cheapest_price(prices):
    return min(prices, key=lambda x: x["amount"]) if prices else None


def has_vcpu_memory(details):
    return "vcpu" in details and "memory" in details


from decimal import Decimal, ROUND_DOWN


def round_to_three_decimals(value):
    # Convert to Decimal
    decimal_value = Decimal(value)

    # If the value is less than 1 and not zero, handle the first non-zero digit
    if 0 < decimal_value < 1:
        # Convert to scientific notation to find the first non-zero digit
        value_scientific = format(decimal_value, ".6e")
        exponent = int(value_scientific.split("e")[-1])

        # If the exponent is significantly small, handle as a special case
        if exponent <= -6:
            rounded_value = decimal_value
        else:
            # Calculate the number of decimal places to keep
            decimal_places = abs(exponent) + 2  # 2 more than the exponent
            quantize_pattern = Decimal("1e-" + str(decimal_places))

            # Rounding the value
            rounded_value = decimal_value.quantize(
                quantize_pattern, rounding=ROUND_DOWN
            )
    else:
        # If the value is 1 or more, or exactly 0, round to a maximum of three decimal places
        rounded_value = decimal_value.quantize(Decimal("0.001"), rounding=ROUND_DOWN)

    return rounded_value
