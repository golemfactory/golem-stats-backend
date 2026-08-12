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
    data_fresh = serializers.BooleanField(source="is_fresh", read_only=True)

    # Everything here describes what the provider is offering *right now*. When
    # no recent scan backs that up we report unknown (null) rather than serving
    # the last value we happen to have stored, which may be months old. The row
    # itself is kept: this is a reporting rule, not a deletion.
    UNKNOWN_WHEN_STALE = (
        "properties",
        "monthly_price_glm",
        "monthly_price_usd",
        "hourly_price_usd",
        "hourly_price_glm",
        "is_overpriced",
        "overpriced_compared_to",
        "suggest_env_per_hour_price",
        "times_more_expensive",
        "cheaper_than",
        "times_cheaper",
    )

    class Meta:
        model = Offer
        fields = [
            "runtime",
            "monthly_price_glm",
            "properties",
            "updated_at",
            "last_seen_at",
            "data_fresh",
            "monthly_price_usd",
            "hourly_price_usd",
            "hourly_price_glm",
            "is_overpriced",
            "overpriced_compared_to",
            "suggest_env_per_hour_price",
            "times_more_expensive",
            "cheaper_than",
            "times_cheaper",
        ]

    def to_representation(self, instance):
        data = super().to_representation(instance)
        if not instance.is_fresh:
            for field in self.UNKNOWN_WHEN_STALE:
                data[field] = None
        return data


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
            "network",
        ]

    def get_offers(self, node):
        offers = Offer.objects.filter(provider=node)
        return {offer.runtime: OfferSerializer(offer).data for offer in offers}

    def get_uptime(self, node):
        return calculate_uptime_percentage(node.node_id, node)
