import gzip
import json
from unittest.mock import AsyncMock, patch

from django.test import TestCase

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
