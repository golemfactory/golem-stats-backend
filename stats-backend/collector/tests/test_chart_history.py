import json
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from django.test import TestCase

from api2 import tasks
from collector.models import NetworkStats


class ChartHistoryTests(TestCase):
    def setUp(self):
        self.now = datetime(2026, 9, 10, 12, tzinfo=timezone.utc)
        self.cache = {}
        self.redis_patch = patch.object(tasks, "r")
        redis = self.redis_patch.start()
        self.addCleanup(self.redis_patch.stop)
        redis.get.side_effect = self.cache.get
        redis.set.side_effect = self.cache.__setitem__
        clock = patch.object(tasks.timezone, "now", return_value=self.now)
        clock.start()
        self.addCleanup(clock.stop)

    def test_capacity_month_keeps_older_runtime_and_week_stays_bounded(self):
        for days, runtime in [(31, "expired"), (20, "old-runtime"), (2, "vm"), (0, "vm")]:
            row = NetworkStats.objects.create(
                online=10, cores=20, memory=1024, disk=2048, runtime=runtime
            )
            NetworkStats.objects.filter(pk=row.pk).update(date=self.now - timedelta(days=days))
        tasks.network_stats_combined_hourly()
        tasks.network_stats_combined_5min()
        data = json.loads(self.cache["network_historical_stats_combined"])
        self.assertNotIn("expired", data)
        self.assertEqual(len(data["old-runtime"]["30d"]["date"]), 1)
        self.assertEqual(data["old-runtime"]["7d"]["date"], [])
        self.assertEqual(len(data["vm"]["24h"]["date"]), 1)
        self.assertEqual(len(data["vm"]["7d"]["date"]), 2)

    def test_pricing_month_and_week_have_distinct_windows(self):
        def series(network, points):
            return [{"date": point.timestamp(), "average_cpu": 1} for point in points]
        with patch.object(tasks, "_pricing_windowed_series", side_effect=series):
            tasks.pricing_combined_hourly()
            tasks.pricing_combined_5min()
        for frames in json.loads(self.cache["pricing_data_combined"]).values():
            self.assertEqual(len(frames["30d"]), 721)
            self.assertEqual(len(frames["7d"]), 169)
            self.assertEqual(len(frames["24h"]), 289)
            self.assertEqual(frames["30d"][0]["date"], (self.now - timedelta(days=30)).timestamp())

    def test_versions_include_month_only_versions_with_aligned_series(self):
        old = {"date": int((self.now - timedelta(days=20)).timestamp()), "0.17.7": 10}
        recent = {"date": int(self.now.timestamp()), "0.17.10": 20}
        with patch.object(tasks, "_version_count_series", side_effect=[[old, recent], [recent]]) as query:
            tasks.network_versions_combined_hourly()
            tasks.network_versions_combined_5min()
        self.assertEqual(query.call_args_list[0].args[0], int((self.now - timedelta(days=30)).timestamp()))
        data = json.loads(self.cache["network_versions_combined"])
        self.assertEqual(data["versions"], ["0.17.10", "0.17.7"])
        self.assertEqual(len(data["30d"]), 2)
        self.assertEqual(len(data["7d"]), 1)
        self.assertEqual(data["24h"][0]["0.17.7"], 0)
        self.assertEqual(data["30d"][0]["0.17.10"], 0)
