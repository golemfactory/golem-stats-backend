import gzip
import json
from datetime import timedelta
from unittest.mock import AsyncMock, patch

from django.test import TestCase
from django.utils import timezone

from collector.models import NetworkStats


class CompressedRedisEndpointTestsMixin:
    """Shared cases for endpoints serving compact redis JSON with gzip."""

    url = None
    payload = None

    def _mock_aioredis(self, aioredis_mock, content):
        aioredis_mock.Redis.return_value.get = AsyncMock(return_value=content)

    @patch("api2.views.aioredis")
    def test_serves_gzip_when_accepted(self, aioredis_mock):
        self._mock_aioredis(aioredis_mock, self.payload)
        response = self.client.get(self.url, HTTP_ACCEPT_ENCODING="gzip")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response["Content-Encoding"], "gzip")
        self.assertEqual(
            json.loads(gzip.decompress(response.content)), json.loads(self.payload)
        )

    @patch("api2.views.aioredis")
    def test_serves_plain_json_without_gzip(self, aioredis_mock):
        self._mock_aioredis(aioredis_mock, self.payload)
        response = self.client.get(self.url, HTTP_ACCEPT_ENCODING="identity")
        self.assertEqual(response.status_code, 200)
        self.assertNotIn("Content-Encoding", response)
        self.assertEqual(json.loads(response.content), json.loads(self.payload))

    @patch("api2.views.aioredis")
    def test_503_when_key_missing(self, aioredis_mock):
        self._mock_aioredis(aioredis_mock, None)
        response = self.client.get(self.url)
        self.assertEqual(response.status_code, 503)


class NetworkHistoricalStatsCompressedTests(
    CompressedRedisEndpointTestsMixin, TestCase
):
    url = "/v2/network/historical/stats/compressed"
    payload = json.dumps(
        {"vm": {"1d": {"date": [1.0], "online": [5]}}}, separators=(",", ":")
    )


class Ec2ComparisonCompressedTests(CompressedRedisEndpointTestsMixin, TestCase):
    url = "/v2/network/comparison/compressed"
    payload = json.dumps(
        {"ec2_instance_name": ["t3.micro"], "ec2_vcpu": [2]}, separators=(",", ":")
    )


class NetworkHistoricalStatsColumnarTaskTests(TestCase):
    @patch("api2.tasks.r")
    def test_writes_columnar_copy_matching_row_format(self, redis_mock):
        from api2.tasks import network_historical_stats_to_redis_v2

        NetworkStats.objects.create(
            online=3, cores=24, memory=64 * 1024, disk=512 * 1024, runtime="vm", gpus=1
        )
        network_historical_stats_to_redis_v2()

        stored = {
            call.args[0]: json.loads(call.args[1])
            for call in redis_mock.set.call_args_list
        }
        rows = stored["network_historical_stats_v2"]
        columnar = stored["network_historical_stats_v2_columnar"]

        self.assertEqual(set(rows.keys()), set(columnar.keys()))
        for runtime, intervals in rows.items():
            for key, row_list in intervals.items():
                for field in ["date", "online", "cores", "memory", "disk", "gpus"]:
                    self.assertEqual(
                        columnar[runtime][key][field],
                        [row[field] for row in row_list],
                    )


class OfferFreshnessTests(TestCase):
    """Stale rows stay in the database but must not be reported as current."""

    VM_PROPERTIES = {
        "golem.runtime.name": "vm",
        "golem.inf.cpu.threads": 8,
        "golem.inf.mem.gib": 16.0,
        "golem.com.usage.vector": [
            "golem.usage.duration_sec",
            "golem.usage.cpu_sec",
        ],
        "golem.com.pricing.model.linear.coeffs": [0.000001, 0.000002, 0.0],
        "node_id": "0xabc",
        "wallet": "0xwallet",
    }

    def setUp(self):
        from api2.models import GLM, Node, Offer

        GLM.objects.create(id=1, current_price=0.1)
        self.node = Node.objects.create(node_id="0xabc", type="provider")
        self.offer = Offer.objects.create(
            provider=self.node,
            runtime="vm",
            properties=dict(self.VM_PROPERTIES),
            monthly_price_glm=100.0,
            hourly_price_usd=0.5,
        )

    def _scan_payload(self, **overrides):
        data = dict(self.VM_PROPERTIES)
        data.update(overrides)
        return [json.dumps(data)]

    def test_fresh_queryset_excludes_never_seen_and_expired_offers(self):
        from api2.models import Offer

        self.assertEqual(Offer.objects.fresh().count(), 0)

        self.offer.last_seen_at = timezone.now() - timedelta(hours=1)
        self.offer.save()
        self.assertEqual(Offer.objects.fresh().count(), 0)

        self.offer.last_seen_at = timezone.now()
        self.offer.save()
        self.assertEqual(Offer.objects.fresh().count(), 1)

    def test_serializer_reports_unknown_instead_of_stale_values(self):
        from api2.serializers import OfferSerializer

        data = OfferSerializer(self.offer).data
        self.assertFalse(data["data_fresh"])
        self.assertIsNone(data["properties"])
        self.assertIsNone(data["monthly_price_glm"])
        self.assertIsNone(data["hourly_price_usd"])
        self.assertIsNone(data["last_seen_at"])
        # The row itself is untouched.
        self.offer.refresh_from_db()
        self.assertEqual(self.offer.properties, self.VM_PROPERTIES)
        self.assertEqual(self.offer.monthly_price_glm, 100.0)

    def test_serializer_reports_values_once_a_scan_has_seen_the_offer(self):
        from api2.serializers import OfferSerializer

        self.offer.last_seen_at = timezone.now()
        self.offer.save()

        data = OfferSerializer(self.offer).data
        self.assertTrue(data["data_fresh"])
        self.assertEqual(data["properties"], self.VM_PROPERTIES)
        self.assertEqual(data["monthly_price_glm"], 100.0)

    def test_scan_marks_unchanged_offer_as_seen(self):
        """An offer that did not change still has to count as observed."""
        from api2.scanner import update_providers_info

        update_providers_info(self._scan_payload())

        self.offer.refresh_from_db()
        self.assertEqual(self.offer.properties, self.VM_PROPERTIES)  # unchanged
        self.assertIsNotNone(self.offer.last_seen_at)
        self.assertTrue(self.offer.is_fresh)

    def test_scan_persists_prices_for_a_newly_created_offer(self):
        from api2.models import Offer

        from api2.scanner import update_providers_info

        Offer.objects.all().delete()
        update_providers_info(self._scan_payload())

        offer = Offer.objects.get(provider=self.node, runtime="vm")
        self.assertIsNotNone(offer.monthly_price_glm)
        self.assertIsNotNone(offer.hourly_price_usd)
        self.assertIsNotNone(offer.last_seen_at)

    def test_scan_advances_updated_at_when_the_offer_changes(self):
        from api2.scanner import update_providers_info

        self.offer.last_seen_at = timezone.now()
        self.offer.save()
        before = self.offer.updated_at

        update_providers_info(self._scan_payload(**{"golem.inf.cpu.threads": 16}))

        self.offer.refresh_from_db()
        self.assertEqual(self.offer.properties["golem.inf.cpu.threads"], 16)
        self.assertGreater(self.offer.updated_at, before)


class OfferVariantSelectionTests(TestCase):
    """A scan carrying old+new versions of the same offer must pick the newer,
    and keep picking it, instead of alternating by arrival order."""

    def setUp(self):
        from api2.models import GLM, Node

        GLM.objects.create(id=1, current_price=0.1)
        self.node = Node.objects.create(node_id="0xabc", type="provider")

    def _payload(self, coeffs):
        return json.dumps(
            {
                "golem.runtime.name": "vm",
                "golem.inf.cpu.threads": 8,
                "golem.inf.mem.gib": 16.0,
                "golem.com.usage.vector": [
                    "golem.usage.duration_sec",
                    "golem.usage.cpu_sec",
                ],
                "golem.com.pricing.model.linear.coeffs": coeffs,
                "node_id": "0xabc",
                "wallet": "0xwallet",
            }
        )

    def test_float_noise_is_the_same_variant(self):
        from api2.scanner import offer_content_hash

        a = json.loads(self._payload([2.75e-06, 2.75e-06, 0.0]))
        b = json.loads(self._payload([2.7500000000000004e-06, 2.7500000000000004e-06, 0.0]))
        self.assertEqual(offer_content_hash(a), offer_content_hash(b))

    def test_newer_variant_wins_regardless_of_arrival_order(self):
        from api2.models import Offer, OfferVariantSighting
        from api2.scanner import update_providers_info

        old = self._payload([1e-06, 1e-06, 0.0])
        # The old variant has been on the market for a while.
        update_providers_info([old])
        OfferVariantSighting.objects.update(
            first_seen_at=timezone.now() - timedelta(days=1),
            last_seen_at=timezone.now() - timedelta(days=1) + timedelta(hours=23),
        )

        new = self._payload([2e-06, 2e-06, 0.0])
        # Both arrive in one scan, stale copy last — it must still lose.
        update_providers_info([new, old])
        offer = Offer.objects.get(provider=self.node, runtime="vm")
        self.assertEqual(
            offer.properties["golem.com.pricing.model.linear.coeffs"],
            [2e-06, 2e-06, 0.0],
        )

        # And again with the opposite arrival order: no flip-flop.
        update_providers_info([old, new])
        offer.refresh_from_db()
        self.assertEqual(
            offer.properties["golem.com.pricing.model.linear.coeffs"],
            [2e-06, 2e-06, 0.0],
        )

    def test_tied_variants_resolve_deterministically(self):
        from api2.models import Offer
        from api2.scanner import update_providers_info

        a = self._payload([1e-06, 1e-06, 0.0])
        b = self._payload([2e-06, 2e-06, 0.0])
        # Both first seen in the same scan: winner is arbitrary but must be
        # the same one every scan afterwards.
        update_providers_info([a, b])
        first = Offer.objects.get(provider=self.node, runtime="vm").properties

        for batch in ([b, a], [a, b], [b, a]):
            update_providers_info(batch)
            again = Offer.objects.get(provider=self.node, runtime="vm").properties
            self.assertEqual(again, first)

    def test_sightings_are_pruned_after_a_day(self):
        from api2.models import OfferVariantSighting
        from api2.scanner import update_providers_info

        update_providers_info([self._payload([1e-06, 1e-06, 0.0])])
        OfferVariantSighting.objects.update(
            last_seen_at=timezone.now() - timedelta(days=2))

        update_providers_info([self._payload([2e-06, 2e-06, 0.0])])
        remaining = set(
            OfferVariantSighting.objects.values_list("content_hash", flat=True))
        from api2.scanner import offer_content_hash

        self.assertEqual(
            remaining,
            {offer_content_hash(json.loads(self._payload([2e-06, 2e-06, 0.0])))},
        )
