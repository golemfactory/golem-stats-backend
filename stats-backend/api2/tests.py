import gzip
import json
from unittest.mock import AsyncMock, patch

from django.test import TestCase, override_settings

from collector.models import NetworkStats, Requestors


@override_settings(SALAD_REQUESTOR_TOKEN="secret-token")
class SubmitRequestorNodesTests(TestCase):
    def setUp(self):
        self.url = "/v2/requestors/submit"
        self.auth_header = {"HTTP_AUTHORIZATION": "Bearer secret-token"}

    def test_rejects_missing_token(self):
        response = self.client.post(
            self.url,
            data=json.dumps({"node_ids": ["node-1"]}),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 401)

    def test_creates_requestors(self):
        response = self.client.post(
            self.url,
            data=json.dumps({"node_ids": ["node-1", "node-2"]}),
            content_type="application/json",
            **self.auth_header,
        )
        self.assertEqual(response.status_code, 201)
        self.assertEqual(Requestors.objects.count(), 2)
        self.assertEqual(response.json().get("created"), 2)
        self.assertEqual(response.json().get("existing"), 0)

    def test_counts_existing_and_duplicates(self):
        Requestors.objects.create(node_id="node-1")

        response = self.client.post(
            self.url,
            data=json.dumps({"node_ids": ["node-1", "node-3", "node-3"]}),
            content_type="application/json",
            **self.auth_header,
        )

        self.assertEqual(response.status_code, 201)
        self.assertEqual(Requestors.objects.count(), 2)
        self.assertEqual(response.json().get("created"), 1)
        self.assertEqual(response.json().get("existing"), 2)


class NetworkHistoricalStatsCompressedTests(TestCase):
    url = "/v2/network/historical/stats/compressed"
    payload = json.dumps(
        {"vm": {"1d": {"date": [1.0], "online": [5]}}}, separators=(",", ":")
    )

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
