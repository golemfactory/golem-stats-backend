# import fakeredis
# from mock import patch
# from django.http import HttpRequest
# from django.test import SimpleTestCase
# import json
# from collector.models import Node
# from datetime import datetime, timedelta, date


# class APITests(SimpleTestCase):

#     def test_offerscraper(self):
#         r = fakeredis.FakeStrictRedis()
#         data = json.dumps({'golem.activity.caps.transfer.protocol': ['https', 'gftp', 'http'], 'golem.com.payment.debit-notes.accept-timeout?': 240, 'golem.com.payment.platform.erc20-mainnet-glm.address': '0x6a8f9da445748b9ee8af26c00b1b4839fe7230d3', 'golem.com.payment.platform.zksync-mainnet-glm.address': '0x6a8f9da445748b9ee8af26c00b1b4839fe7230d3', 'golem.com.pricing.model': 'linear', 'golem.com.pricing.model.linear.coeffs': [2.777777777777778e-06, 0.0, 0.0], 'golem.com.scheme': 'payu', 'golem.com.scheme.payu.interval_sec': 120.0, 'golem.com.usage.vector': [
#             'golem.usage.cpu_sec', 'golem.usage.duration_sec'], 'golem.inf.cpu.architecture': 'x86_64', 'golem.inf.cpu.cores': 16, 'golem.inf.cpu.threads': 30, 'golem.inf.mem.gib': 130.0, 'golem.inf.storage.gib': 36.20123291015625, 'golem.node.debug.subnet': 'public-beta', 'golem.node.id.name': 'serv', 'golem.runtime.name': 'wasmtime', 'golem.runtime.version': '0.2.1', 'golem.srv.caps.multi-activity': True, 'wallet': '0x6a8f9da445748b9ee8af26c00b1b4839fe7230d3', 'id': '0x6a8f9da445748b9ee8af26c00b1b4839fe7230d3'})
#         r.set("offers", data)
#         content = r.get("offers")
#         serialized = json.loads(content)
#         print(serialized)
#         for line in serialized:
#             data = json.loads(line)
#             provider = data['id']
#             wallet = data['wallet']
#             obj, created = Node.objects.get_or_create(node_id=provider)
#             if created:
#                 obj.data = data
#                 obj.wallet = wallet
#                 obj.online = True
#                 obj.updated_at = datetime.now()
#                 obj.save(update_fields=[
#                          'data', 'wallet', 'online', 'updated_at'])
#             else:
#                 obj.data = data
#                 obj.wallet = wallet
#                 obj.online = True
#                 obj.updated_at = datetime.now()
#                 obj.save(update_fields=[
#                          'data', 'wallet', 'online', 'updated_at'])
#         self.assertEqual(
#             obj.node_id, "0x6a8f9da445748b9ee8af26c00b1b4839fe7230d3")
