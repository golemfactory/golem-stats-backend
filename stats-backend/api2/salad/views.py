import json

import aioredis
from django.http import HttpResponse, JsonResponse

from api2.salad.tasks import is_salad_enabled


async def get_redis_data(key: str) -> dict | list | None:
    pool = aioredis.ConnectionPool.from_url(
        "redis://redis:6379/0", decode_responses=True
    )
    r = aioredis.Redis(connection_pool=pool)
    try:
        content = await r.get(key)
        if content:
            return json.loads(content)
        return None
    finally:
        pool.disconnect()


async def current_stats(request):
    """
    Returns the complete current Salad network stats snapshot.

    GET /v2/partner/salad/network/stats

    Response includes:
    - timestamp: When stats were collected
    - network_id: "salad"
    - providers: {online, computing}
    - resources: {cores, memory_gib, disk_gib, gpus}
    - earnings: {6h, 24h, 168h, 720h, 2160h, total}
    - versions: Array of version distributions
    """
    if request.method != "GET":
        return HttpResponse(status=400)

    data = await get_redis_data("salad:current_stats")
    if data is None:
        return JsonResponse(
            {"error": "Salad stats not available"},
            status=503,
            json_dumps_params={"indent": 4},
        )
    return JsonResponse(data, safe=False, json_dumps_params={"indent": 4})


async def online_stats(request):
    """
    Returns online network stats for Salad.
    Matches format of /v1/network/online/stats

    GET /v2/partner/salad/network/online/stats
    """
    if request.method != "GET":
        return HttpResponse(status=400)

    data = await get_redis_data("salad:online_stats")
    if data is None:
        return JsonResponse(
            {"error": "Salad stats not available"},
            status=503,
            json_dumps_params={"indent": 4},
        )
    return JsonResponse(data, safe=False, json_dumps_params={"indent": 4})


async def network_utilization(request):
    """
    Returns network utilization (providers computing) over time.
    Matches format of /v1/network/utilization

    GET /v2/partner/salad/network/utilization
    """
    if request.method != "GET":
        return HttpResponse(status=400)

    data = await get_redis_data("salad:network_utilization")
    if data is None:
        return JsonResponse(
            {"error": "Salad utilization data not available"},
            status=503,
            json_dumps_params={"indent": 4},
        )
    return JsonResponse(data, safe=False, json_dumps_params={"indent": 4})


async def network_versions(request):
    """
    Returns Yagna version distribution for Salad network.
    Matches format of /v1/network/versions

    GET /v2/partner/salad/network/versions
    """
    if request.method != "GET":
        return HttpResponse(status=400)

    data = await get_redis_data("salad:network_versions")
    if data is None:
        return JsonResponse(
            {"error": "Salad version data not available"},
            status=503,
            json_dumps_params={"indent": 4},
        )
    return JsonResponse(data, safe=False, json_dumps_params={"indent": 4})


async def network_earnings_overview(request):
    """
    Returns earnings overview for various time frames.
    Matches format of /v1/network/earnings/overviewnew

    GET /v2/partner/salad/network/earnings/overview
    """
    if request.method != "GET":
        return HttpResponse(status=400)

    data = await get_redis_data("salad:earnings_overview")
    if data is None:
        return JsonResponse(
            {"error": "Salad earnings data not available"},
            status=503,
            json_dumps_params={"indent": 4},
        )
    return JsonResponse(data, safe=False, json_dumps_params={"indent": 4})


async def computing_now(request):
    """
    Returns current number of providers computing.
    Matches format of /v1/network/computing

    GET /v2/partner/salad/network/computing
    """
    if request.method != "GET":
        return HttpResponse(status=400)

    data = await get_redis_data("salad:computing_now")
    if data is None:
        return JsonResponse(
            {"error": "Salad computing data not available"},
            status=503,
            json_dumps_params={"indent": 4},
        )
    return JsonResponse(data, safe=False, json_dumps_params={"indent": 4})


async def historical_stats(request):
    """
    Returns historical network stats by runtime.
    Matches format of /v2/network/historical/stats

    GET /v2/partner/salad/network/historical/stats
    """
    if request.method != "GET":
        return HttpResponse(status=400)

    data = await get_redis_data("salad:network_historical_stats")
    if data is None:
        return JsonResponse(
            {"error": "Salad historical stats not available"},
            status=503,
            json_dumps_params={"indent": 4},
        )
    return JsonResponse(data, safe=False, json_dumps_params={"indent": 4})


async def computing_daily(request):
    """
    Returns daily computing totals over time.
    Matches format of /v2/network/historical/computing

    GET /v2/partner/salad/network/historical/computing
    """
    if request.method != "GET":
        return HttpResponse(status=400)

    data = await get_redis_data("salad:computing_daily")
    if data is None:
        return JsonResponse(
            {"error": "Salad computing daily data not available"},
            status=503,
            json_dumps_params={"indent": 4},
        )
    return JsonResponse(data, safe=False, json_dumps_params={"indent": 4})


async def partner_status(request):
    """
    Returns integration status for Salad partner.
    Used for monitoring the integration health.

    GET /v2/network/partner/status
    """
    if request.method != "GET":
        return HttpResponse(status=400)

    enabled = is_salad_enabled()

    if not enabled:
        return JsonResponse(
            {
                "enabled": False,
                "cached": False,
                "network_id": "salad",
                "timestamp": None,
                "providers_online": 0,
                "total_cores": 0,
                "total_memory_gib": 0,
                "error": "Integration not configured (missing SALAD_API_URL or SALAD_API_TOKEN)",
            },
            json_dumps_params={"indent": 4},
        )

    # Check fetch status
    fetch_status = await get_redis_data("salad:fetch_status")
    current_stats = await get_redis_data("salad:current_stats")

    if current_stats is None:
        return JsonResponse(
            {
                "enabled": True,
                "cached": False,
                "network_id": "salad",
                "timestamp": None,
                "providers_online": 0,
                "total_cores": 0,
                "total_memory_gib": 0,
                "error": (
                    fetch_status.get("error") if fetch_status else "No data available"
                ),
            },
            json_dumps_params={"indent": 4},
        )

    resources = current_stats.get("resources", {})
    providers = current_stats.get("providers", {})

    return JsonResponse(
        {
            "enabled": True,
            "cached": True,
            "network_id": current_stats.get("network_id", "salad"),
            "timestamp": current_stats.get("timestamp"),
            "providers_online": providers.get("online", 0),
            "total_cores": resources.get("cores", 0),
            "total_memory_gib": resources.get("memory_gib", 0),
        },
        json_dumps_params={"indent": 4},
    )
