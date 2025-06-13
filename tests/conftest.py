# ABOUTME: Provides test fixtures for the Firestore Schema Explorer tests.
# ABOUTME: Contains mocks for Firestore clients, documents, and collections.

import pytest
from unittest.mock import MagicMock
from datetime import datetime
from google.cloud import firestore
from google.cloud.firestore_v1.base_document import DocumentSnapshot
from google.cloud.firestore_v1.collection import CollectionReference
from google.api_core.exceptions import PermissionDenied


@pytest.fixture
def mock_firestore_client():
    """Mock Firestore client with basic functionality."""
    mock_client = MagicMock(spec=firestore.Client)
    mock_client.project = "test-project"
    return mock_client


@pytest.fixture
def mock_empty_collection():
    """Mock an empty collection."""
    mock_collection = MagicMock(spec=CollectionReference)
    mock_collection.id = "empty_collection"
    mock_collection.path = "empty_collection"
    mock_collection.stream.return_value = []
    mock_collection.limit.return_value = mock_collection
    return mock_collection


@pytest.fixture
def mock_document_data():
    """Mock document data with various field types."""
    return {
        "string_field": "test string",
        "int_field": 42,
        "float_field": 3.14,
        "bool_field": True,
        "null_field": None,
        "timestamp_field": datetime.now(),
        "array_simple": [1, 2, 3],
        "array_mixed": [1, "string", True],
        "array_empty": [],
        "map_field": {"nested_string": "nested value", "nested_int": 100},
        "deep_nested": {"level1": {"level2": {"level3": "deep value"}}},
    }


@pytest.fixture
def mock_document(mock_document_data):
    """Mock a Firestore document with test data."""
    mock_doc = MagicMock(spec=DocumentSnapshot)
    mock_doc.id = "test_doc_id"
    mock_doc.to_dict.return_value = mock_document_data

    # Set up mock reference for subcollections
    mock_doc.reference = MagicMock()
    mock_doc.reference.collections.return_value = []

    return mock_doc


@pytest.fixture
def mock_collection_with_docs(mock_document):
    """Mock a collection with documents."""
    mock_collection = MagicMock(spec=CollectionReference)
    mock_collection.id = "test_collection"
    mock_collection.path = "test_collection"
    mock_collection.stream.return_value = [mock_document]
    mock_collection.limit.return_value = mock_collection
    return mock_collection


@pytest.fixture
def mock_collection_with_subcollections(mock_document):
    """Mock a collection with documents that have subcollections."""
    mock_subcollection = MagicMock(spec=CollectionReference)
    mock_subcollection.id = "subcollection"
    mock_subcollection.stream.return_value = []
    mock_subcollection.limit.return_value = mock_subcollection

    # Set up the document with a subcollection
    mock_doc = mock_document
    mock_doc.reference.path = "test_collection/test_doc_id"
    mock_doc.reference.collections.return_value = [mock_subcollection]

    # Set up the collection
    mock_collection = MagicMock(spec=CollectionReference)
    mock_collection.id = "test_collection"
    mock_collection.path = "test_collection"
    mock_collection.stream.return_value = [mock_doc]
    mock_collection.limit.return_value = mock_collection

    return mock_collection


@pytest.fixture
def mock_permission_denied_collection():
    """Mock a collection that raises PermissionDenied when streaming."""
    mock_collection = MagicMock(spec=CollectionReference)
    mock_collection.id = "secure_collection"
    mock_collection.path = "secure_collection"
    mock_collection.stream.side_effect = PermissionDenied("Permission denied")
    mock_collection.limit.return_value = mock_collection
    return mock_collection


@pytest.fixture
def firestore_with_collections(
    mock_firestore_client,
    mock_empty_collection,
    mock_collection_with_docs,
    mock_collection_with_subcollections,
    mock_permission_denied_collection,
):
    """Set up a Firestore client with various test collections."""
    # Configure the collections method to return different collections
    mock_firestore_client.collections.return_value = [
        mock_empty_collection,
        mock_collection_with_docs,
        mock_collection_with_subcollections,
        mock_permission_denied_collection,
    ]

    # Configure collection method to return the appropriate mock based on the path
    def get_collection(path):
        if path == "empty_collection":
            return mock_empty_collection
        elif path == "test_collection":
            return mock_collection_with_subcollections
        elif path == "test_collection/test_doc_id/subcollection":
            # Return a fresh subcollection mock for the actual subcollection path
            subcol_mock = MagicMock(spec=CollectionReference)
            subcol_mock.id = "subcollection"
            subcol_mock.stream.return_value = []
            subcol_mock.limit.return_value = subcol_mock
            return subcol_mock
        elif path == "secure_collection":
            return mock_permission_denied_collection
        return MagicMock()

    mock_firestore_client.collection.side_effect = get_collection

    return mock_firestore_client
