from rest_framework import serializers
from .models import Node, Offer


class OfferSerializer(serializers.ModelSerializer):

    class Meta:
        model = Offer
        fields = ['runtime', 'monthly_price_glm', 'properties', 'updated_at']


class NodeSerializer(serializers.ModelSerializer):
    runtimes = serializers.SerializerMethodField('get_offers')

    class Meta:
        model = Node
        fields = ['earnings_total', 'node_id',
                  'online', 'version', 'updated_at', 'created_at', 'runtimes', 'computing_now']

    def get_offers(self, node):
        offers = Offer.objects.filter(
            provider=node)
        data = {}
        for obj in offers:
            data[obj.runtime] = {
                'monthly_price_glm': obj.monthly_price_glm,
                'updated_at': obj.updated_at,
                'properties': obj.properties
            }
        return data
