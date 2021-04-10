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


@api_view(['GET', ])
def online_nodes(request):
    """
    List all online nodes.
    """
    if request.method == 'GET':
        data = Node.objects.filter(online=True)
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
        cores = []
        threads = []
        memory = []
        disk = []
        query = Node.objects.filter(online=True)
        for obj in query:
            cores.append(obj.data['golem.inf.cpu.cores'])
            threads.append(obj.data['golem.inf.cpu.threads'])
            memory.append(obj.data['golem.inf.mem.gib'])
            disk.append(obj.data['golem.inf.storage.gib'])
        content = {'online': len(query), 'cores': sum(
            cores), 'threads': sum(threads), 'memory': sum(memory), 'disk': sum(disk)}
        return Response(content, status=status.HTTP_200_OK)
    else:
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)


@api_view(['GET'])
def network_utilization(request, start, end):
    """
    Queries the networks utilization from a start date to the end date specified, and returns
    timestamps in ms along with providers computing.
    """
    if request.method == 'GET':
        time_difference = end - start
        if not time_difference > 300000:
            domain = os.environ.get(
                'STATS_URL') + f"api/datasources/proxy/40/api/v1/query_range?query=sum(activity_provider_created%7Bjob%3D~%22community.1%22%7D%20-%20activity_provider_destroyed%7Bjob%3D~%22community.1%22%7D)&start={start}&end={end}&step=30"
            data = get_stats_data(domain)
            return Response(data)
        else:
            content = {
                'reason': 'The queried time range cannot surpass 300000 seconds'}
            return Response(content, status=status.HTTP_400_BAD_REQUEST)


@api_view(['GET'])
def providers_computing_currently(request):
    """
    Returns how many providers are currently computing a task.
    """
    if request.method == 'GET':
        end = round(time.time())
        start = round(time.time()) - int(10)
        domain = os.environ.get(
            'STATS_URL') + f"api/datasources/proxy/40/api/v1/query_range?query=sum(activity_provider_created%7Bjob%3D~%22community.1%22%7D%20-%20activity_provider_destroyed%7Bjob%3D~%22community.1%22%7D)&start={start}&end={end}&step=1"
        data = get_stats_data(domain)
        print(data)
        content = {'computing_now': data['data']['result'][0]['values'][-1][1]}
        return Response(content, status=status.HTTP_200_OK)


@api_view(['GET'])
def providers_average_earnings(request):
    """
    Returns providers average earnings per task in the last hour.
    """
    if request.method == 'GET':
        end = round(time.time())
        start = round(time.time()) - int(10)
        domain = os.environ.get(
            'STATS_URL') + f"api/datasources/proxy/40/api/v1/query_range?query=avg(payment_amount_received%7Bjob%3D~%22community.1%22%7D%2F10%5E9)&start={start}&end={end}&step=1"
        data = get_stats_data(domain)
        print(data)
        content = {'average_earnings': data['data']
                   ['result'][0]['values'][-1][1][0:5]}
        return Response(content, status=status.HTTP_200_OK)


@api_view(['GET'])
def network_earnings(request, hours):
    """
    Returns the earnings for the whole network the last n hours.
    """
    if request.method == 'GET':
        end = round(time.time())
        start = round(time.time()) - int(10)
        domain = os.environ.get(
            'STATS_URL') + f"api/datasources/proxy/40/api/v1/query_range?query=sum(increase(payment_amount_received%7Bjob%3D~%22community.1%22%7D%5B{hours}h%5D)%2F10%5E9)&start={start}&end={end}&step=1"
        data = get_stats_data(domain)
        content = {'total_earnings': data['data']
                   ['result'][0]['values'][-1][1][0:6]}
        return Response(content, status=status.HTTP_200_OK)
