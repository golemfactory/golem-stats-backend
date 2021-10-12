from rest_framework import serializers
from collector.models import Node, NetworkStatsMax, NetworkMedianPricingMax, NetworkAveragePricingMax, ProvidersComputingMax, NetworkStats, Requestors


class NodeSerializer(serializers.ModelSerializer):

    class Meta:
        model = Node
        fields = ['earnings_total', 'node_id', 'data',
                  'online', 'version', 'updated_at', 'created_at', 'benchmark_score']


class RequestorSerializer(serializers.ModelSerializer):

    class Meta:
        model = Requestors
        fields = ['tasks_requested']


class NetworkStatsMaxSerializer(serializers.ModelSerializer):

    class Meta:
        model = NetworkStatsMax
        fields = ['online', 'cores',  'memory', 'disk', 'date']


class NetworkStatsSerializer(serializers.ModelSerializer):

    class Meta:
        model = NetworkStats
        fields = ['online', 'cores',  'memory', 'disk', 'date']


class NetworkMedianPricingMaxSerializer(serializers.ModelSerializer):

    class Meta:
        model = NetworkMedianPricingMax
        fields = ['start', 'cpuh',  'perh', 'date']


class NetworkAveragePricingMaxSerializer(serializers.ModelSerializer):

    class Meta:
        model = NetworkAveragePricingMax
        fields = ['start', 'cpuh',  'perh', 'date']


class ProvidersComputingMaxSerializer(serializers.ModelSerializer):

    class Meta:
        model = ProvidersComputingMax
        fields = ['total', 'date']
