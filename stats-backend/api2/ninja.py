from typing import List, Optional

from django.conf import settings
from ninja import NinjaAPI, Schema
from ninja.security import HttpBearer

from collector.models import Requestors


class BearerTokenAuth(HttpBearer):
    def authenticate(self, request, token: str):
        expected = getattr(settings, "SALAD_REQUESTOR_TOKEN", None)
        if expected and token == expected:
            return token
        return None


api = NinjaAPI(urls_namespace="api2_ninja", docs_url=None)


class SubmitRequestorsIn(Schema):
    node_ids: Optional[List[str]] = None
    node_id: Optional[str] = None


@api.post(
    "/requestors/submit",
    auth=BearerTokenAuth(),
    response={201: dict, 400: dict, 401: dict, 503: dict},
)
def submit_requestor_nodes(request, payload: SubmitRequestorsIn):
    if not getattr(settings, "SALAD_REQUESTOR_TOKEN", None):
        return api.create_response(request, {"detail": "Server missing token configuration"}, status=503)

    raw_ids: List[str] = []
    if payload.node_ids:
        raw_ids.extend(payload.node_ids)
    if payload.node_id:
        raw_ids.append(payload.node_id)

    cleaned = [node_id.strip() for node_id in raw_ids if isinstance(node_id, str) and node_id.strip()]
    if not cleaned:
        return api.create_response(request, {"detail": "Provide node_ids (string or list)"}, status=400)

    unique_ids = set(cleaned)
    existing_db = set(
        Requestors.objects.filter(node_id__in=unique_ids).values_list("node_id", flat=True)
    )

    duplicates_in_payload = len(cleaned) - len(unique_ids)
    new_ids = unique_ids - existing_db

    Requestors.objects.bulk_create([Requestors(node_id=node_id) for node_id in new_ids])

    created = len(new_ids)
    existing = len(existing_db) + duplicates_in_payload

    return api.create_response(
        request,
        {"created": created, "existing": existing, "received": len(cleaned)},
        status=201,
    )
