import json

from django.test import TestCase, override_settings

from collector.models import Requestors


@override_settings(SALAD_REQUESTOR_TOKEN="secret-token")
class SubmitRequestorNodesTests(TestCase):
    def setUp(self):
        self.url = "/v2/requestors/submit"
        self.auth_header = {"HTTP_AUTHORIZATION": "Bearer secret-token"}

    def test_rejects_missing_token(self):
        response = self.client.post(
            self.url,
            data=json.dumps({"node_ids": ["node-1"]}),
            content_type="application/json",
        )
        self.assertEqual(response.status_code, 401)

    def test_creates_requestors(self):
        response = self.client.post(
            self.url,
            data=json.dumps({"node_ids": ["node-1", "node-2"]}),
            content_type="application/json",
            **self.auth_header,
        )
        self.assertEqual(response.status_code, 201)
        self.assertEqual(Requestors.objects.count(), 2)
        self.assertEqual(response.json().get("created"), 2)
        self.assertEqual(response.json().get("existing"), 0)

    def test_counts_existing_and_duplicates(self):
        Requestors.objects.create(node_id="node-1")

        response = self.client.post(
            self.url,
            data=json.dumps({"node_ids": ["node-1", "node-3", "node-3"]}),
            content_type="application/json",
            **self.auth_header,
        )

        self.assertEqual(response.status_code, 201)
        self.assertEqual(Requestors.objects.count(), 2)
        self.assertEqual(response.json().get("created"), 1)
        self.assertEqual(response.json().get("existing"), 2)
