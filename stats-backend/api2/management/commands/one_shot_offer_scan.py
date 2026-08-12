import asyncio
import time

import requests
from django.core.management.base import BaseCommand

from api2.scanner import monitor_nodes_status

YAGNA_API = "http://127.0.0.1:7465"


class Command(BaseCommand):
    """Run one market scan per subnet against the local yagna, then exit.

    This is the entrypoint of the one-shot offer_scanner containers: the
    container boots a yagna daemon, runs this command once, and dies. Swarm's
    restart_policy respawns it, which is what makes scanning periodic — there
    is deliberately no resident daemon and no self-requeuing celery chain
    (the old v2_offer_scraper chain broke silently whenever a worker died
    mid-scan, because the task only re-queued itself after finishing).
    """

    help = "Run one market scan of each subnet against the local yagna, then exit."

    def add_arguments(self, parser):
        parser.add_argument(
            "--subnets",
            nargs="*",
            default=["public", "ray-on-golem-heads"],
        )

    def handle(self, *args, **options):
        self._wait_for_yagna()

        async def scan_all():
            # One daemon serves several concurrent demand subscriptions, so
            # the subnets scan in parallel instead of stacking their 60s
            # windows.
            await asyncio.gather(
                *(monitor_nodes_status(subnet) for subnet in options["subnets"])
            )

        asyncio.run(scan_all())
        self.stdout.write("scan complete")

    def _wait_for_yagna(self, timeout=90):
        """Block until the daemon's REST API answers; any HTTP response counts."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                requests.get(f"{YAGNA_API}/version/get", timeout=2)
                return
            except requests.exceptions.RequestException:
                time.sleep(2)
        raise SystemExit("yagna REST API did not come up in time")
