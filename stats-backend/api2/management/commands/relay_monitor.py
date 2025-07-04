# yourapp/management/commands/relay_monitor.py

import asyncio
import aiohttp
import requests
from datetime import datetime
from django.core.management.base import BaseCommand
from django.db.models import Q
from api2.models import Node
from api2.tasks import bulk_update_node_statuses

class Command(BaseCommand):
    help = 'Monitors relay nodes and listens for events'

    yacn2_base_url = "http://yacn2.dev.golem.network:9000"
    golembase_base_url = "http://ya-golembase.dev.golem.network:8000"

    def log_message(self, message, is_error=False):
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        if is_error:
            self.stdout.write(self.style.ERROR(f"[{timestamp}] {message}"))
        else:
            self.stdout.write(f"[{timestamp}] {message}")

    def handle(self, *args, **options):
        self.log_message('Starting relay monitor...')
        asyncio.run(self.main())

    async def main(self):
        await self.initial_relay_nodes_scan()
        await self.listen_for_relay_events()

    async def initial_relay_nodes_scan(self):
        self.log_message("Starting initial relay nodes scan...")
        nodes_to_update = {}

        # Scan both sources
        yacn2_nodes = self.scan_yacn2_nodes()
        golembase_nodes = self.scan_golembase_nodes()

        # Merge results
        nodes_to_update.update(yacn2_nodes)
        nodes_to_update.update(golembase_nodes)

        self.log_message("Querying database for online providers...")
        online_providers = set(Node.objects.filter(online=True).values_list('node_id', flat=True))

        offline_count = 0
        for provider_id in online_providers:
            if provider_id not in nodes_to_update:
                nodes_to_update[provider_id] = False
                offline_count += 1

        self.log_message(f"Found {offline_count} providers to mark as offline")

        nodes_to_update_list = list(nodes_to_update.items())
        self.log_message(f"Scheduling bulk update for {len(nodes_to_update_list)} nodes")

        if nodes_to_update_list:
            bulk_update_node_statuses.delay(nodes_to_update_list)

        self.log_message("Initial relay nodes scan completed")

    def scan_yacn2_nodes(self):
        nodes = {}
        self.log_message("Scanning yacn2 nodes...")
        for prefix in range(256):
            try:
                response = requests.get(f"{self.yacn2_base_url}/nodes/{prefix:02x}", timeout=5)
                response.raise_for_status()
                data = response.json()
                for node_id, sessions in data.items():
                    node_id = node_id.strip().lower()
                    is_online = bool(sessions) and any('seen' in item for item in sessions if item)
                    nodes[node_id] = is_online
            except requests.RequestException as e:
                self.log_message(f"Error fetching data for prefix {prefix:02x} from yacn2: {e}", is_error=True)
        return nodes

    def scan_golembase_nodes(self):
        nodes = {}
        self.log_message("Scanning golembase nodes...")
        try:
            response = requests.get(f"{self.golembase_base_url}/nodes", timeout=10)
            response.raise_for_status()
            data = response.json()
            for root_key, sessions in data.items():
                if root_key == 'count':
                    continue

                all_ids = {root_key.strip().lower()}
                is_online = bool(sessions) and any('seen' in item for item in sessions if item)

                if is_online:
                    for session in sessions:
                        if 'id' in session:
                            all_ids.add(session['id'].strip().lower())
                        if 'identities' in session and isinstance(session['identities'], list):
                            for identity in session['identities']:
                                all_ids.add(identity.strip().lower())

                    for node_id in all_ids:
                        nodes[node_id] = True
        except requests.RequestException as e:
            self.log_message(f"Error fetching data from golembase: {e}", is_error=True)
        return nodes

    async def listen_for_relay_events(self):
        self.log_message('Listening for relay events from all sources...')
        await asyncio.gather(
            self.listen_to_event_source(f"{self.yacn2_base_url}/events"),
            self.listen_to_event_source(f"{self.golembase_base_url}/events")
        )

    async def listen_to_event_source(self, url):
        async with aiohttp.ClientSession() as session:
            while True:
                try:
                    async with session.get(url) as resp:
                        self.log_message(f"Connected to event stream at {url} with status {resp.status}")
                        async for line in resp.content:
                            if line:
                                try:
                                    decoded_line = line.decode('utf-8').strip()
                                    if decoded_line.startswith('event:'):
                                        event_type = decoded_line.split(':', 1)[1].strip()
                                    elif decoded_line.startswith('data:'):
                                        node_id = decoded_line.split(':', 1)[1].strip()
                                        event = {'Type': event_type, 'Id': node_id}
                                        await self.process_event(event)
                                except Exception as e:
                                    self.log_message(f"Failed to process event from {url}: {e}", is_error=True)
                except Exception as e:
                    self.log_message(f"Connection error at {url}: {e}", is_error=True)
                    self.log_message("Attempting to reconnect in 5 seconds...")
                    await asyncio.sleep(5)

    async def process_event(self, event):
        event_type = event.get('Type')
        node_id = event.get('Id')

        if not node_id:
            return

        if event_type == 'new-node':
            self.log_message(f"New node detected: {node_id}")
            bulk_update_node_statuses.delay([(node_id, True)])
        elif event_type == 'lost-node':
            self.log_message(f"Node lost: {node_id}")
            bulk_update_node_statuses.delay([(node_id, False)])