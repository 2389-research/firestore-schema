# ABOUTME: Tests the collection processing functionality.
# ABOUTME: Validates handling of collections, documents, and subcollections.

import pytest
from unittest.mock import patch
from pathlib import Path

import sys

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from main import FirestoreSchemaExplorer


class TestCollectionProcessing:
    """Test the collection processing functionality."""

    @pytest.fixture
    def explorer(self, firestore_with_collections):
        """Create a FirestoreSchemaExplorer instance with mock collections."""
        with patch("main.firestore.Client", return_value=firestore_with_collections):
            with patch("main.load_dotenv"):
                explorer = FirestoreSchemaExplorer(max_depth=3)
                # Clear progress indicators for testing
                explorer._progress = None
                return explorer

    def test_empty_collection(self, explorer):
        """Test processing of an empty collection."""
        output = explorer.process_collection("empty_collection")

        assert len(output) == 2
        assert "### Collection: `empty_collection`" in output[0]
        assert "*No documents found*" in output[1]

        # Stats should reflect a collection was processed but no documents
        assert explorer.stats["collections"] == 1
        assert explorer.stats["documents"] == 0

    def test_collection_with_docs(self, explorer):
        """Test processing of a collection with documents."""
        # Reset stats before test
        explorer.stats["collections"] = 0
        explorer.stats["documents"] = 0
        explorer.stats["fields"] = 0

        output = explorer.process_collection("test_collection")

        # Verify collection header
        assert "### Collection: `test_collection`" in output[0]

        # Verify document header
        assert "#### Document: `test_doc_id`" in output[1]

        # Check a collection was processed
        assert explorer.stats["collections"] > 0
        assert explorer.stats["documents"] > 0
        assert explorer.stats["fields"] > 0  # Should have processed some fields

    def test_collection_with_subcollections(self, explorer):
        """Test processing of collections with subcollections."""
        # Set max_depth to ensure subcollections are processed
        explorer.max_depth = 3

        output = explorer.process_collection("test_collection")

        # Should include main collection, document, and subcollection
        assert any("### Collection: `test_collection`" in line for line in output)
        assert any("#### Document: `test_doc_id`" in line for line in output)
        assert any(
            "### Collection: `test_collection/test_doc_id/subcollection`" in line
            for line in output
        )

        # Stats should reflect multiple collections
        assert explorer.stats["collections"] >= 2

    def test_max_depth_limit(self, explorer):
        """Test enforcement of max_depth when processing collections."""
        # Set max_depth to 1 to prevent subcollection processing
        explorer.max_depth = 1

        output = explorer.process_collection("test_collection")

        # Should include main collection and document
        assert any("### Collection: `test_collection`" in line for line in output)
        assert any("#### Document: `test_doc_id`" in line for line in output)

        # Should NOT include subcollection
        assert not any("subcollection" in line for line in output)

    def test_permission_denied(self, explorer):
        """Test handling of permission denied errors."""
        output = explorer.process_collection("secure_collection")

        # Should include collection but not access documents
        assert any("### Collection: `secure_collection`" in line for line in output)
        assert any(
            "*Permission denied when accessing documents*" in line for line in output
        )

    def test_cycle_prevention(self, explorer):
        """Test prevention of processing cycles."""
        # First process the collection normally
        explorer.process_collection("test_collection")

        # Reset stats to clearly see if processing happens again
        explorer.stats["collections"] = 0
        explorer.stats["documents"] = 0

        # Try to process the same collection again - should be skipped
        output = explorer.process_collection("test_collection")

        # Should return empty list and not increase stats
        assert output == []
        assert explorer.stats["collections"] == 0
        assert explorer.stats["documents"] == 0
