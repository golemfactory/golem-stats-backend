from rest_framework import serializers
from collector.models import Node


class NodeSerializer(serializers.ModelSerializer):

    class Meta:
        model = Node
        fields = ['earnings_total', 'node_id', 'data', 'online', 'updated_at']
