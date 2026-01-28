import json
import logging
import os
from datetime import datetime

import redis
import requests
from celery import shared_task
from django.conf import settings

from core.celery import app

logger = logging.getLogger(__name__)

pool = redis.ConnectionPool(host="redis", port=6379, db=0)
r = redis.Redis(connection_pool=pool)

# Redis keys for Salad data
SALAD_CURRENT_STATS_KEY = "salad:current_stats"
SALAD_HISTORICAL_STATS_KEY = "salad:historical_stats"
SALAD_LAST_FETCH_KEY = "salad:last_fetch"
SALAD_FETCH_STATUS_KEY = "salad:fetch_status"


class SaladConfigError(Exception):
    pass


def get_salad_api_config():
    """Get Salad API configuration from environment/settings.
    
    Raises:
        SaladConfigError: If SALAD_API_URL or SALAD_API_TOKEN is not set.
    """
    base_url = os.environ.get("SALAD_API_URL")
    token = os.environ.get("SALAD_API_TOKEN")
    
    if not base_url or not token:
        raise SaladConfigError(
            "Salad integration requires SALAD_API_URL and SALAD_API_TOKEN environment variables"
        )
    
    return {
        "base_url": base_url.rstrip("/"),
        "token": token,
    }


def is_salad_enabled() -> bool:
    return bool(os.environ.get("SALAD_API_URL") and os.environ.get("SALAD_API_TOKEN"))


def make_salad_request(endpoint: str) -> tuple[dict | None, int]:
    config = get_salad_api_config()

    url = f"{config['base_url']}{endpoint}"
    headers = {
        "Authorization": f"Bearer {config['token']}",
        "Content-Type": "application/json",
    }

    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        return response.json(), response.status_code
    except requests.exceptions.Timeout:
        logger.error(f"Timeout fetching Salad API: {endpoint}")
        return None, 408
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP error fetching Salad API: {e}")
        return None, e.response.status_code if e.response else 500
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching Salad API: {e}")
        return None, 500
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON from Salad API: {e}")
        return None, 500


@app.task(bind=True, max_retries=3, default_retry_delay=60)
def fetch_salad_current_stats(self):
    try:
        data, status_code = make_salad_request("/v1/network/stats")

        if data is None:
            logger.warning(f"Failed to fetch Salad current stats: {status_code}")
            r.set(
                SALAD_FETCH_STATUS_KEY,
                json.dumps(
                    {
                        "success": False,
                        "error": f"HTTP {status_code}",
                        "timestamp": datetime.utcnow().isoformat() + "Z",
                    }
                ),
            )
            # Retry on transient errors
            if status_code in [408, 500, 502, 503, 504]:
                raise self.retry(exc=Exception(f"Salad API error: {status_code}"))
            return

        # Validate response structure
        required_keys = [
            "timestamp",
            "network_id",
            "providers",
            "resources",
            "earnings",
            "versions",
        ]
        if not all(key in data for key in required_keys):
            logger.error(f"Invalid Salad stats response structure: missing keys")
            return

        # Store in Redis
        r.set(SALAD_CURRENT_STATS_KEY, json.dumps(data))
        r.set(SALAD_LAST_FETCH_KEY, datetime.utcnow().isoformat() + "Z")
        r.set(
            SALAD_FETCH_STATUS_KEY,
            json.dumps(
                {
                    "success": True,
                    "timestamp": datetime.utcnow().isoformat() + "Z",
                    "network_id": data.get("network_id"),
                    "providers_online": data.get("providers", {}).get("online", 0),
                }
            ),
        )

        logger.info(
            f"Salad stats fetched: {data.get('providers', {}).get('online', 0)} providers online"
        )

        # Process derived data from current stats
        _process_current_stats_derived_data(data)

    except Exception as e:
        logger.exception(f"Exception fetching Salad current stats: {e}")
        r.set(
            SALAD_FETCH_STATUS_KEY,
            json.dumps(
                {
                    "success": False,
                    "error": str(e),
                    "timestamp": datetime.utcnow().isoformat() + "Z",
                }
            ),
        )
        raise


@app.task(bind=True, max_retries=3, default_retry_delay=120)
def fetch_salad_historical_stats(self):
    try:
        data, status_code = make_salad_request("/v1/network/stats/historical")

        if data is None:
            logger.warning(f"Failed to fetch Salad historical stats: {status_code}")
            # Retry on transient errors
            if status_code in [408, 500, 502, 503, 504]:
                raise self.retry(exc=Exception(f"Salad API error: {status_code}"))
            return

        # Validate response structure
        required_keys = [
            "network_id",
            "network_stats",
            "utilization",
            "computing_daily",
        ]
        if not all(key in data for key in required_keys):
            logger.error(f"Invalid Salad historical stats response structure")
            return

        # Store in Redis
        r.set(SALAD_HISTORICAL_STATS_KEY, json.dumps(data))

        logger.info(
            f"Salad historical stats fetched: {len(data.get('utilization', []))} utilization points"
        )

        # Process derived data from historical stats
        _process_historical_stats_derived_data(data)

    except Exception as e:
        logger.exception(f"Exception fetching Salad historical stats: {e}")
        raise


# Helper functions to process derived data inline


def _process_current_stats_derived_data(data: dict):
    """Process current stats into derived Redis keys."""
    try:
        resources = data.get("resources", {})
        providers = data.get("providers", {})
        earnings = data.get("earnings", {})
        versions_raw = data.get("versions", [])

        # Online stats
        online_stats = {
            "online": providers.get("online", 0),
            "cores": resources.get("cores", 0),
            "threads": resources.get("cores", 0),
            "memory": resources.get("memory_gib", 0),
            "disk": resources.get("disk_gib", 0),
            "gpus": resources.get("gpus", 0),
        }
        r.set("salad:online_stats", json.dumps(online_stats))

        # Earnings overview
        earnings_overview = {
            "network_earnings_6h": {"total_earnings": earnings.get("6h", 0)},
            "network_earnings_24h": {"total_earnings": earnings.get("24h", 0)},
            "network_earnings_168h": {"total_earnings": earnings.get("168h", 0)},
            "network_earnings_720h": {"total_earnings": earnings.get("720h", 0)},
            "network_earnings_2160h": {"total_earnings": earnings.get("2160h", 0)},
            "network_total_earnings": {"total_earnings": earnings.get("total", 0)},
        }
        r.set("salad:earnings_overview", json.dumps(earnings_overview))

        # Computing now
        computing_now = {"computing_now": str(providers.get("computing", 0))}
        r.set("salad:computing_now", json.dumps(computing_now))

        # Versions
        total_count = sum(v.get("count", 0) for v in versions_raw)
        versions = []
        for v in versions_raw:
            count = v.get("count", 0)
            percentage = round((count / total_count) * 100, 2) if total_count > 0 else 0
            versions.append(
                {
                    "version": v.get("version", "unknown"),
                    "count": str(count),
                    "rc": v.get("rc", False),
                    "percentage": percentage,
                }
            )
        r.set("salad:network_versions", json.dumps(versions))

    except Exception as e:
        logger.exception(f"Error processing current stats derived data: {e}")


def _process_historical_stats_derived_data(data: dict):
    """Process historical stats into derived Redis keys."""
    try:
        network_stats = data.get("network_stats", {})
        utilization = data.get("utilization", [])
        computing_daily = data.get("computing_daily", [])

        # Utilization
        formatted_utilization = {
            "data": {"result": [{"values": utilization}] if utilization else []}
        }
        r.set("salad:network_utilization", json.dumps(formatted_utilization))

        # Historical network stats - filter into time buckets
        now = datetime.utcnow().timestamp()
        time_thresholds = {
            "1d": now - 86400,  # 1 day
            "7d": now - 604800,  # 7 days
            "1m": now - 2592000,  # 30 days
            "1y": now - 31536000,  # 365 days
        }

        formatted_data = {}
        for runtime, points in network_stats.items():
            formatted_data[runtime] = {
                "1d": [],
                "7d": [],
                "1m": [],
                "1y": [],
                "All": [],
            }
            sorted_points = sorted(points, key=lambda x: x.get("date", 0))
            for point in sorted_points:
                point_date = point.get("date", 0)
                formatted_point = {
                    "date": point_date,
                    "online": point.get("online", 0),
                    "cores": point.get("cores", 0),
                    "memory": point.get("memory_gib", 0),
                    "disk": point.get("disk_gib", 0),
                    "gpus": point.get("gpus", 0),
                }
                # Add to All bucket
                formatted_data[runtime]["All"].append(formatted_point)
                # Add to time-filtered buckets
                for bucket, threshold in time_thresholds.items():
                    if point_date >= threshold:
                        formatted_data[runtime][bucket].append(formatted_point)

        r.set("salad:network_historical_stats", json.dumps(formatted_data))

        # Computing daily
        r.set("salad:computing_daily", json.dumps(computing_daily))

    except Exception as e:
        logger.exception(f"Error processing historical stats derived data: {e}")
