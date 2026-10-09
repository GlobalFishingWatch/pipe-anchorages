import pytest

from gfw.common.bigquery.helper import BigQueryHelper


@pytest.fixture
def bq_clients():
    """Mock BigQuery clients created by a pipeline, to inspect what it sent to BigQuery."""
    return []


@pytest.fixture
def bq_factory_kwargs():
    """Keyword arguments each mock BigQuery client was created with."""
    return []


@pytest.fixture
def bq_client_factory(bq_clients, bq_factory_kwargs):
    """A bq_client_factory to inject into a pipeline's run(), recording the clients it makes."""
    mock_factory = BigQueryHelper.get_client_factory(mocked=True)

    def factory(**kwargs):
        client = mock_factory(**kwargs)
        bq_clients.append(client)
        bq_factory_kwargs.append(kwargs)
        return client

    return factory
