# ABOUTME: Tests the type detection and description functionality.
# ABOUTME: Validates correct identification of Firestore data types.

import pytest
from datetime import datetime
from unittest.mock import MagicMock, patch
from pathlib import Path

import sys

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from main import FirestoreSchemaExplorer
from google.cloud import firestore


class TestTypeDescriptions:
    """Test the type description functionality."""

    @pytest.fixture
    def explorer(self, mock_firestore_client):
        """Create a FirestoreSchemaExplorer instance for testing."""
        with patch("main.firestore.Client", return_value=mock_firestore_client):
            with patch("main.load_dotenv"):
                return FirestoreSchemaExplorer()

    def test_basic_types(self, explorer):
        """Test description of basic data types."""
        assert explorer.describe_type("test string") == "string"
        assert explorer.describe_type(42) == "integer"
        assert explorer.describe_type(3.14) == "float"
        assert explorer.describe_type(True) == "boolean"
        assert explorer.describe_type(None) == "null"

    def test_collection_types(self, explorer):
        """Test description of collection types."""
        assert explorer.describe_type({"key": "value"}) == "map"
        assert explorer.describe_type([]) == "array<?>"

        # With sampling enabled (default)
        assert explorer.describe_type([1, 2, 3]) == "array<integer>"
        assert (
            explorer.describe_type([1, "string", True])
            == "array<mixed:boolean,integer,string>"
        )

        # With sampling disabled
        explorer.sample_arrays = False
        assert explorer.describe_type([1, 2, 3]) == "array<integer>"
        assert explorer.describe_type([1, "string", True]) == "array<integer>"

    def test_timestamp(self, explorer):
        """Test timestamp detection."""
        # Create a mock timestamp
        mock_timestamp = MagicMock()
        mock_timestamp.timestamp = datetime.now()

        assert explorer.describe_type(mock_timestamp) == "timestamp"
        assert explorer.describe_type(datetime.now()) == "timestamp"

    def test_geopoint(self, explorer):
        """Test geopoint detection."""
        # Create a mock geopoint that looks like a GeoPoint
        mock_geopoint = MagicMock(spec=firestore.GeoPoint)
        mock_geopoint.latitude = 37.7749
        mock_geopoint.longitude = -122.4194
        # Remove timestamp attribute to avoid confusion
        if hasattr(mock_geopoint, "timestamp"):
            delattr(mock_geopoint, "timestamp")

        assert explorer.describe_type(mock_geopoint) == "geopoint"

        # Test with actual firestore GeoPoint if possible
        geopoint = firestore.GeoPoint(37.7749, -122.4194)
        assert explorer.describe_type(geopoint) == "geopoint"

    def test_reference(self, explorer):
        """Test document reference detection."""
        # Create a mock document reference
        mock_ref = MagicMock(spec=firestore.DocumentReference)
        mock_ref.path = "test_collection/test_doc_id"
        # Remove timestamp attribute to avoid confusion
        if hasattr(mock_ref, "timestamp"):
            delattr(mock_ref, "timestamp")

        assert (
            explorer.describe_type(mock_ref) == "reference→test_collection/test_doc_id"
        )

    def test_array_sampling(self, explorer):
        """Test array sampling with different sizes."""
        explorer.sample_arrays = True

        # Test with array_sample_size
        explorer.array_sample_size = 2
        large_mixed_array = [1, 2, "string", True, False, 3.14]
        # Should only sample the first 2 elements - both of which are integers
        assert explorer.describe_type(large_mixed_array) == "array<integer>"

        # Test with larger sample size
        explorer.array_sample_size = 4
        # Should sample the first 4 elements
        assert (
            explorer.describe_type(large_mixed_array)
            == "array<mixed:boolean,integer,string>"
        )
