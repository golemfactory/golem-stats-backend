from rest_framework import serializers
from .models import Node, Offer


class OfferSerializer(serializers.ModelSerializer):

    class Meta:
        model = Offer
        fields = ['runtime', 'properties', 'updated_at']


class NodeSerializer(serializers.ModelSerializer):
    offers = serializers.SerializerMethodField('get_offers')

    class Meta:
        model = Node
        fields = ['earnings_total', 'node_id',
                  'online', 'version', 'updated_at', 'created_at', 'offers', 'computing_now']

    def get_offers(self, node):
        offers = Offer.objects.filter(
            provider=node)
        serializer = OfferSerializer(instance=offers, many=True)
        return serializer.data
