from rest_framework import serializers
from .models import Node, Offer, EC2Instance, NodeStatusHistory
from .scoring import calculate_uptime_percentage


class EC2InstanceSerializer(serializers.ModelSerializer):
    class Meta:
        model = EC2Instance
        fields = "__all__"


class OfferSerializer(serializers.ModelSerializer):
    overpriced_compared_to = EC2InstanceSerializer(read_only=True)
    cheaper_than = EC2InstanceSerializer(
        read_only=True
    )  # Serialize the cheaper_than field

    class Meta:
        model = Offer
        fields = [
            "runtime",
            "monthly_price_glm",
            "properties",
            "updated_at",
            "monthly_price_usd",
            "is_overpriced",
            "overpriced_compared_to",
            "suggest_env_per_hour_price",
            "times_more_expensive",
            "cheaper_than",
            "times_cheaper",
        ]


class NodeSerializer(serializers.ModelSerializer):
    runtimes = serializers.SerializerMethodField("get_offers")
    uptime = serializers.SerializerMethodField("get_uptime")

    class Meta:
        model = Node
        fields = [
            "uptime",
            "earnings_total",
            "node_id",
            "online",
            "version",
            "updated_at",
            "created_at",
            "runtimes",
            "computing_now",
            "wallet",
        ]

    def get_offers(self, node):
        offers = Offer.objects.filter(provider=node)
        return {offer.runtime: OfferSerializer(offer).data for offer in offers}

    def get_uptime(self, node):
        return calculate_uptime_percentage(node.node_id, node)
