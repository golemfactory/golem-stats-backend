from django.test import TestCase
from django.template.defaultfilters import slugify
from collector.models import Node, Benchmark, Network, NetworkStatsMax, NetworkStats, ProvidersComputing, ProvidersComputingMax, NetworkMedianPricing, NetworkAveragePricing, NetworkMedianPricingMax, NetworkAveragePricingMax, Requestors, requestor_scraper_check
from datetime import datetime, timedelta, date


class ModelsTest(TestCase):

    def setUp(self):
        Node.objects.create(
            node_id="0xa2a7596528d1d91045ff2e1edc4ad01e42ed49ue", wallet="0xa2a7596528d1d91045ff2e1edc4ad01e42ed49db", earnings_total=32.1, online=True, version="1.0.0", )
        Network.objects.create(total_earnings=302.3)
        NetworkStatsMax.objects.create(
            online=500, cores=19200, memory=1000, disk=2000, date=date.today())
        NetworkStats.objects.create(
            online=400, cores=18000, memory=1000, disk=2000)
        ProvidersComputing.objects.create(total=98)
        ProvidersComputingMax.objects.create(total=90, date=date.today())
        NetworkMedianPricing.objects.create(start=10.04, cpuh=4.38, perh=77.9)
        NetworkMedianPricingMax.objects.create(
            start=8.14, cpuh=4.38, perh=2.9, date=date.today())
        NetworkAveragePricing.objects.create(
            start=2.77, cpuh=5.55, perh=7729.9)
        NetworkAveragePricingMax.objects.create(
            start=2.77, cpuh=8.22, perh=7729.9, date=date.today())
        Requestors.objects.create(
            node_id="0xa2a7596528d1d91045ff2e1edc4ad01e42ed49ue", tasks_requested=8.12)

    def test_node(self):
        """The model returns the correct integer."""
        obj = Node.objects.get(
            node_id="0xa2a7596528d1d91045ff2e1edc4ad01e42ed49ue")
        self.assertEqual(obj.online, True)
        filter = Node.objects.filter(
            online=True)
        for node in filter:
            self.assertIn(
                node.wallet, '0xa2a7596528d1d91045ff2e1edc4ad01e42ed49db')

    def test_benchmark(self):
        """A benchmark with foreingKey to provider works"""
        node = Node.objects.get(
            node_id="0xa2a7596528d1d91045ff2e1edc4ad01e42ed49ue")
        obj = Benchmark.objects.create(benchmark_score=300, provider=node)
        self.assertEqual(obj.provider, node)
        self.assertEqual(obj.benchmark_score, 300)

    def test_network_total_earnings(self):
        obj = Network.objects.get(id=1)
        self.assertEqual(obj.total_earnings, 302.3)

    def test_networkstatsmax(self):
        obj = NetworkStatsMax.objects.get(id=1)
        self.assertEqual(obj.online, 500)

    def test_networkstats(self):
        obj = NetworkStats.objects.get(id=1)
        self.assertEqual(obj.online, 400)

    def test_providerscomputing(self):
        obj = ProvidersComputing.objects.get(id=1)
        self.assertEqual(obj.total, 98)

    def test_providerscomputingmax(self):
        obj = ProvidersComputingMax.objects.get(id=1)
        self.assertEqual(obj.total, 90)

    def test_networkmedianpricing(self):
        obj = NetworkMedianPricing.objects.get(id=1)
        self.assertEqual(obj.cpuh, 4.38)

    def test_networkmedianpricingmax(self):
        obj = NetworkMedianPricingMax.objects.get(id=1)
        self.assertEqual(obj.cpuh, 4.38)

    def test_networkaveragepricing(self):
        obj = NetworkAveragePricing.objects.get(id=1)
        self.assertEqual(obj.perh, 7729.9)

    def test_requestors(self):
        obj = Requestors.objects.get(id=1)
        self.assertEqual(obj.tasks_requested, 8.12)

    def test_requestor_scraper_check(self):
        obj = requestor_scraper_check.objects.create(indexed_before=True)
        self.assertEqual(obj.indexed_before, True)
