from django.test import TestCase
from django.template.defaultfilters import slugify
from api.models import APIHits, APICounter


class ModelsTest(TestCase):

    def test_apihits(self):
        """The model returns the correct integer."""
        obj = APIHits.objects.create(count=491291)
        self.assertEqual(obj.count, 491291)

    def test_apicounter(self):
        """The model returns the correct endpoint"""
        obj = APICounter.objects.create(endpoint="The Test Endpoint")
        self.assertEqual(obj.endpoint, "The Test Endpoint")
