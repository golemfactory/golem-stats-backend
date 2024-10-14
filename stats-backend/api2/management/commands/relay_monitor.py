# yourapp/management/commands/relay_monitor.py

import asyncio
import aiohttp
import requests
from django.core.management.base import BaseCommand
from django.db.models import Q
from api2.models import Node
from api2.tasks import bulk_update_node_statuses

class Command(BaseCommand):
    help = 'Monitors relay nodes and listens for events'

    def handle(self, *args, **options):
        self.stdout.write('Starting relay monitor...')
        asyncio.run(self.main())

    async def main(self):
        await self.initial_relay_nodes_scan()
        await self.listen_for_relay_events()

    async def initial_relay_nodes_scan(self):
        base_url = "http://yacn2.dev.golem.network:9000/nodes/"
        nodes_to_update = {}

        for prefix in range(256):
            try:
                response = requests.get(f"{base_url}{prefix:02x}", timeout=5)
                response.raise_for_status()
                data = response.json()

                for node_id, sessions in data.items():
                    node_id = node_id.strip().lower()
                    is_online = bool(sessions) and any('seen' in item for item in sessions if item)
                    nodes_to_update[node_id] = is_online

            except requests.RequestException as e:
                print(f"Error fetching data for prefix {prefix:02x}: {e}")

        # Query the database for all online providers
        online_providers = set(Node.objects.filter(online=True).values_list('node_id', flat=True))

        # Check for providers that are marked as online in the database but not in the relay data
        for provider_id in online_providers:
            if provider_id not in nodes_to_update:
                nodes_to_update[provider_id] = False

        # Convert the dictionary to a list of tuples
        nodes_to_update_list = list(nodes_to_update.items())

        bulk_update_node_statuses.delay(nodes_to_update_list)

    async def listen_for_relay_events(self):
        self.stdout.write('Listening for relay events...')
        url = "http://yacn2.dev.golem.network:9000/events"
        async with aiohttp.ClientSession() as session:
            while True:
                try:
                    async with session.get(url) as resp:
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
                                    self.stdout.write(self.style.ERROR(f"Failed to process event: {e}"))
                except Exception as e:
                    self.stdout.write(self.style.ERROR(f"Connection error: {e}"))
                    await asyncio.sleep(5)  # Wait before reconnecting

    async def process_event(self, event):
        event_type = event.get('Type')
        node_id = event.get('Id')

        if event_type == 'new-node':
            self.stdout.write(f"New node: {node_id}")
            bulk_update_node_statuses.delay([(node_id, True)])
        elif event_type == 'lost-node':
            self.stdout.write(f"Lost node: {node_id}")
            bulk_update_node_statuses.delay([(node_id, False)])

    async def fetch(self, url):
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=5) as response:
                response.raise_for_status()
                return await response.json()