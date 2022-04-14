from django.shortcuts import render
from .models import Node
from .serializers import NodeSerializer
import redis
import json
import aioredis
from asgiref.sync import sync_to_async
from django.http import JsonResponse, HttpResponse

pool = redis.ConnectionPool(host='redis', port=6379, db=0)
r = redis.Redis(connection_pool=pool)


@sync_to_async
def get_node(yagna_id):
    data = Node.objects.filter(node_id=yagna_id)
    return data


async def node(request, yagna_id):
    if request.method == 'GET':
        if yagna_id.startswith("0x"):
            data = await get_node(yagna_id)
            if data:
                serializer = NodeSerializer(data, many=True)
                return JsonResponse(serializer.data, safe=False, json_dumps_params={'indent': 4})
            else:
                return HttpResponse(status=404)
        else:
            return HttpResponse(status=404)
    else:
        return HttpResponse(status=400)


async def network_online(request):
    if request.method == 'GET':
        pool = aioredis.ConnectionPool.from_url(
            "redis://redis:6379/0", decode_responses=True
        )
        r = aioredis.Redis(connection_pool=pool)
        content = await r.get("online")
        data = json.loads(content)
        pool.disconnect()
        return JsonResponse(data, safe=False, json_dumps_params={'indent': 4})
    else:
        return HttpResponse(status=400)
