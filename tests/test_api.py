import os

import pytest
from flask import url_for

from application import app
from mock import patch


@pytest.fixture
def client():
    with app.test_request_context():
        app.config['TESTING'] = True
        client = app.test_client()
        yield client


def test_get_product(client):
    sample_es_response = {
        "hits": {
            "hits": [{
                "_source": {
                    "brand": "sample brand",
                    "sku": "sample sku"
            }}]
        }
    }
    with patch('elasticsearch.Elasticsearch.search', return_value=sample_es_response):
        assert (sample_es_response["hits"]["hits"][0]["_source"] ==
                client.get(url_for('product', sku='sample')).json)