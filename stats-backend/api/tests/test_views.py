# import fakeredis
# from mock import patch
# from django.http import HttpRequest
# from django.test import SimpleTestCase
# from django.urls import reverse


# class APITests(SimpleTestCase):

#     def test_network_online(self):
#         r = fakeredis.FakeStrictRedis()
#         response = self.client.get(reverse('home'))
#         self.assertEquals(response.status_code, 200)
