"""Unit tests for src/utils/api_client.py"""

import pytest
import responses as rsps
from requests.exceptions import HTTPError

from src.utils.api_client import ApiClient


@pytest.fixture
def client():
    return ApiClient(base_url="https://example.com/api/")


@rsps.activate
def test_get_returns_json(client):
    rsps.add(rsps.GET, "https://example.com/api/action/foo", json={"result": "ok"})
    data = client.get("action/foo")
    assert data == {"result": "ok"}


@rsps.activate
def test_get_raises_on_4xx(client):
    rsps.add(rsps.GET, "https://example.com/api/action/bad", status=404)
    with pytest.raises(HTTPError):
        client.get("action/bad")


@rsps.activate
def test_ckan_resources_returns_list(client):
    rsps.add(
        rsps.GET,
        "https://example.com/api/action/package_show",
        json={"result": {"resources": [{"id": "r1", "name": "file.csv", "url": "https://dl/file.csv"}]}},
    )
    resources = client.ckan_resources("some-package")
    assert isinstance(resources, list)
    assert resources[0]["id"] == "r1"


@rsps.activate
def test_get_url_returns_json(client):
    rsps.add(rsps.GET, "https://other.host/endpoint", json={"foo": "bar"})
    data = client.get_url("https://other.host/endpoint")
    assert data["foo"] == "bar"


def test_auth_headers_set():
    c = ApiClient(base_url="https://x.com", app_id="my-id", app_code="my-code")
    assert c.session.headers["App-Id"] == "my-id"
    assert c.session.headers["App-Code"] == "my-code"


def test_no_auth_headers_when_empty():
    c = ApiClient(base_url="https://x.com")
    assert "App-Id" not in c.session.headers
    assert "App-Code" not in c.session.headers
