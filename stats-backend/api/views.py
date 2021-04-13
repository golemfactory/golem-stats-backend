from rest_framework import status
from rest_framework.decorators import api_view
from rest_framework.response import Response
from .utils import get_stats_data
import os
import time
from collector.models import Node
from .serializers import NodeSerializer
from django.shortcuts import render
from django.db.models import Count
from django.conf import settings
import redis
import json

r = redis.Redis(host='redis', port=6379, db=0)


@api_view(['GET', ])
def online_nodes(request):
    """
    List all online nodes.
    """
    if request.method == 'GET':
        content = r.get("online")
        data = json.loads(content)
        return Response(data, status=status.HTTP_200_OK)
    else:
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


@api_view(['GET', ])
def node(request, yagna_id):
    """
    Retrieves data about a specific node.
    """
    if request.method == 'GET':
        data = Node.objects.filter(node_id=yagna_id)
        serializer = NodeSerializer(data, many=True)
        return Response(serializer.data)
    else:
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


@api_view(['GET', ])
def general_stats(request):
    """
    List network stats for online nodes.
    """
    if request.method == 'GET':
        content = r.get("online_stats")
        data = json.loads(content)
        return Response(data, status=status.HTTP_200_OK)
    else:
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


@api_view(['GET'])
def network_utilization(request, start, end):
    """
    Queries the networks utilization from a start date to the end date specified, and returns
    timestamps in ms along with providers computing.
    """
    if request.method == 'GET':
        content = r.get("network_utilization")
        data = json.loads(content)
        return Response(data, status=status.HTTP_200_OK)


@api_view(['GET'])
def providers_computing_currently(request):
    """
    Returns how many providers are currently computing a task.
    """
    if request.method == 'GET':
        content = r.get("computing_now")
        data = json.loads(content)
        return Response(data, status=status.HTTP_200_OK)


@api_view(['GET'])
def providers_average_earnings(request):
    """
    Returns providers average earnings per task in the last hour.
    """
    if request.method == 'GET':
        content = r.get("provider_average_earnings")
        data = json.loads(content)
        return Response(data, status=status.HTTP_200_OK)


@api_view(['GET'])
def network_earnings_24h(request):
    """
    Returns the earnings for the whole network the last n hours.
    """
    if request.method == 'GET':
        content = r.get("network_earnings_24h")
        data = json.loads(content)
        return Response(data, status=status.HTTP_200_OK)


@api_view(['GET'])
def network_earnings_6h(request):
    """
    Returns the earnings for the whole network the last n hours.
    """
    if request.method == 'GET':
        content = r.get("network_earnings_6h")
        data = json.loads(content)
        return Response(data, status=status.HTTP_200_OK)
