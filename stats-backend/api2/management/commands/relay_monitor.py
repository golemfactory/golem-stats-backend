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
        base_url = "http://yacn2.dev.golem.network:9000/nodes/"
        nodes_to_update = {}

        self.log_message("Starting initial relay nodes scan...")
        for prefix in range(256):
            try:
                response = requests.get(f"{base_url}{prefix:02x}", timeout=5)
                response.raise_for_status()
                data = response.json()
                self.log_message(f"Successfully fetched data for prefix {prefix:02x}")

                for node_id, sessions in data.items():
                    node_id = node_id.strip().lower()
                    is_online = bool(sessions) and any('seen' in item for item in sessions if item)
                    nodes_to_update[node_id] = is_online

            except requests.RequestException as e:
                self.log_message(f"Error fetching data for prefix {prefix:02x}: {e}", is_error=True)

        # Query the database for all online providers
        self.log_message("Querying database for online providers...")
        online_providers = set(Node.objects.filter(online=True).values_list('node_id', flat=True))

        # Check for providers that are marked as online in the database but not in the relay data
        offline_count = 0
        for provider_id in online_providers:
            if provider_id not in nodes_to_update:
                nodes_to_update[provider_id] = False
                offline_count += 1

        self.log_message(f"Found {offline_count} providers to mark as offline")

        # Convert the dictionary to a list of tuples
        nodes_to_update_list = list(nodes_to_update.items())
        self.log_message(f"Scheduling bulk update for {len(nodes_to_update_list)} nodes")

        bulk_update_node_statuses.delay(nodes_to_update_list)
        self.log_message("Initial relay nodes scan completed")

    async def listen_for_relay_events(self):
        self.log_message('Listening for relay events...')
        url = "http://yacn2.dev.golem.network:9000/events"
        async with aiohttp.ClientSession() as session:
            while True:
                try:
                    async with session.get(url) as resp:
                        self.log_message(f"Connected to event stream with status {resp.status}")
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
                                    self.log_message(f"Failed to process event: {e}", is_error=True)
                except Exception as e:
                    self.log_message(f"Connection error: {e}", is_error=True)
                    self.log_message("Attempting to reconnect in 5 seconds...")
                    await asyncio.sleep(5)  # Wait before reconnecting

    async def process_event(self, event):
        event_type = event.get('Type')
        node_id = event.get('Id')

        if event_type == 'new-node':
            self.log_message(f"New node detected: {node_id}")
            bulk_update_node_statuses.delay([(node_id, True)])
        elif event_type == 'lost-node':
            self.log_message(f"Node lost: {node_id}")
            bulk_update_node_statuses.delay([(node_id, False)])

    async def fetch(self, url):
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=5) as response:
                response.raise_for_status()
                return await response.json()