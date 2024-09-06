import subprocess
from django.conf import settings
import os
from .models import Offer, EC2Instance


def identify_network_by_offer(offer):
    for key in settings.GOLEM_MAINNET_KEYS:
        if key in offer.properties:
            return "mainnet"
    for key in settings.GOLEM_TESTNET_KEYS:
        if key in offer.properties:
            return "testnet"
    return "unknown"  # If neither mainnet nor testnet keys are found

def identify_wallet_and_network(event_props):
    for key in settings.GOLEM_MAINNET_KEYS:
        if key in event_props:
            return event_props[key], "mainnet"
    for key in settings.GOLEM_TESTNET_KEYS:
        if key in event_props:
            return event_props[key], "testnet"
    return None, "unknown"


def is_provider_online(provider):
    command = f"yagna net find {provider}"
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    is_online = "Exiting..., error details: Request failed" not in result.stderr
    return is_online


def extract_pricing_from_vm_properties(vm_properties):
    pricing_model = vm_properties.get("golem.com.pricing.model.linear.coeffs", [])
    usage_vector = vm_properties.get("golem.com.usage.vector", [])
    if not usage_vector or not pricing_model:
        return None, None, None

    static_start_price = pricing_model[-1]

    cpu_index = usage_vector.index("golem.usage.cpu_sec")
    cpu_per_hour_price = pricing_model[cpu_index] * 3600

    duration_index = usage_vector.index("golem.usage.duration_sec")
    env_per_hour_price = pricing_model[duration_index] * 3600
    return (
        cpu_per_hour_price,
        env_per_hour_price,
        static_start_price,
    )


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


from celery import shared_task
from decimal import Decimal, ROUND_DOWN
import requests
import os
import time


from core.celery import app
import requests
import os


@app.task(
    bind=True,
    autoretry_for=(requests.exceptions.RequestException,),
    retry_backoff=True,
    max_retries=5,
)
def fetch_and_store_ec2_product_list(self):
    url = "https://api.vantage.sh/v2/products?service_id=aws-ec2"
    headers = headers_setup()

    try:
        response = make_request_with_rate_limit_handling(url, headers, self)
        products_data = response.json().get("products", [])
        for product in products_data:
            process_and_store_product_data.delay(product)
    except requests.RequestException as e:
        raise self.retry(exc=e)


@app.task(
    bind=True,
    autoretry_for=(requests.exceptions.RequestException,),
    retry_backoff=True,
    max_retries=5,
)
def process_and_store_product_data(self, product):
    details = product.get("details", {})
    if not has_vcpu_memory(details):
        return

    product_id, category, name = item_details(product)
    fetch_pricing_data.delay(product_id, category, name, details)


@app.task(
    bind=True,
    autoretry_for=(requests.exceptions.RequestException,),
    retry_backoff=True,
    max_retries=5,
)
def fetch_pricing_data(self, product_id, category, name, details):
    url = f"https://api.vantage.sh/v2/products/{product_id}/prices"
    headers = headers_setup()

    try:
        response = make_request_with_rate_limit_handling(url, headers, self)
        pricing_data = response.json()
        store_ec2_instance_data.delay(pricing_data, product_id, category, name, details)
    except requests.RequestException as e:
        raise self.retry(exc=e)


@app.task
def store_ec2_instance_data(pricing_data, product_id, category, name, details):
    cheapest_price = find_cheapest_price(pricing_data["prices"])
    memory_gb, price = details_conversion(details, cheapest_price)

    # Adjust to match the actual model fields; removed the non-existent 'product_id'
    instance, created = EC2Instance.objects.get_or_create(
        name=name,
        defaults={"vcpu": details["vcpu"], "memory": memory_gb, "price_usd": price},
    )


def make_request_with_rate_limit_handling(url, headers, task_instance=None):
    response = requests.get(url, headers=headers)
    if response.status_code == 429:
        if task_instance:
            reset_time = int(response.headers.get("x-rate-limit-reset", 0))
            current_time = time.time()
            retry_after = max(reset_time - current_time, 1)  # Ensure at least 1 second
            raise task_instance.retry(
                countdown=retry_after, exc=Exception("Rate limit exceeded")
            )
    response.raise_for_status()
    return response


def has_vcpu_memory(details):
    return "vcpu" in details and "memory" in details


def find_cheapest_price(prices):
    return min(prices, key=lambda x: x["amount"]) if prices else None


def details_conversion(details, cheapest_price):
    return float(details["memory"]), (
        cheapest_price["amount"] if cheapest_price else None
    )


def item_details(product):
    return product["id"], product.get("category"), product.get("name")


def headers_setup():
    return {
        "accept": "application/json",
        "authorization": f'Bearer {os.environ.get("VANTAGE_API_KEY")}',
    }
