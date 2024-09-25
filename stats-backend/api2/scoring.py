from .models import Node, Offer, EC2Instance, NodeStatusHistory
from datetime import timedelta
from django.utils import timezone


def calculate_uptime_percentage(node_id, node=None):
    if node is None:
        node = Node.objects.get(node_id=node_id)
    statuses = NodeStatusHistory.objects.filter(node_id=node_id).order_by("timestamp")

    online_duration = timedelta(0)
    last_online_time = None

    for status in statuses:
        if status.is_online:
            last_online_time = status.timestamp
        elif last_online_time:
            online_duration += status.timestamp - last_online_time
            last_online_time = None

    # Check if the node is currently online and add the duration
    if last_online_time is not None:
        online_duration += timezone.now() - last_online_time

    total_duration = timezone.now() - node.uptime_created_at
    uptime_percentage = (
        online_duration.total_seconds() / total_duration.total_seconds()
    ) * 100
    return uptime_percentage
