# ABOUTME: Integration tests for the Firestore Schema Explorer.
# ABOUTME: Tests end-to-end functionality with mocked Firestore clients.

import pytest
import os
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock
import tempfile
import time

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from main import FirestoreSchemaExplorer, main, console


class TestFirestoreIntegration:
    """Integration tests for the FirestoreSchemaExplorer."""

    @pytest.fixture
    def temp_output_file(self):
        """Create a temporary output file for testing."""
        fd, path = tempfile.mkstemp(suffix=".md")
        os.close(fd)
        yield path
        # Clean up
        if os.path.exists(path):
            os.unlink(path)

    def test_end_to_end_with_mock(self, firestore_with_collections, temp_output_file):
        """Test the end-to-end workflow with a mock Firestore client."""
        with patch("main.firestore.Client", return_value=firestore_with_collections):
            with patch("main.load_dotenv"):
                with patch("sys.argv", ["main.py", "--output", temp_output_file]):
                    # Should run without errors
                    assert main() == 0

                    # Verify output file exists and contains expected content
                    assert os.path.exists(temp_output_file)
                    with open(temp_output_file, "r") as f:
                        content = f.read()
                        assert "# 🔥 Firestore Schema Explorer" in content
                        assert "Project: `test-project`" in content
                        assert "empty_collection" in content
                        assert "test_collection" in content

    def test_timeout_handling(self, temp_output_file):
        """Test that the explorer handles timeouts gracefully."""
        # Create a mock client that simulates a hang
        mock_client = MagicMock()

        # Make the collections method hang
        def hanging_collections():
            # Instead of actually sleeping, raise TimeoutError to simulate
            raise TimeoutError("Simulated timeout")

        mock_client.collections.side_effect = hanging_collections
        mock_client.project = "hanging-project"

        with patch("main.firestore.Client", return_value=mock_client):
            with patch("main.load_dotenv"):
                # Create explorer with a short timeout
                explorer = FirestoreSchemaExplorer(timeout=1)

                # This should not hang
                start_time = time.time()
                schema_doc = explorer.explore_database()
                duration = time.time() - start_time

                # Should complete in around 1 second (timeout) plus a small buffer
                assert duration < 5, f"Exploration took too long: {duration} seconds"

                # Should contain timeout message
                assert "*Timed out while listing collections.*" in schema_doc

                # Stats should reflect the timeout
                assert explorer.stats["timeouts"] > 0

    def test_error_handling(self, temp_output_file):
        """Test that the explorer handles various errors gracefully."""
        # Create a mock client that raises errors
        mock_client = MagicMock()
        mock_client.collections.side_effect = Exception("Test error")
        mock_client.project = "error-project"

        with patch("main.firestore.Client", return_value=mock_client):
            with patch("main.load_dotenv"):
                # Create explorer
                explorer = FirestoreSchemaExplorer()

                # This should not crash
                schema_doc = explorer.explore_database()

                # Should contain error message
                assert "*Error listing collections: Test error*" in schema_doc

                # Stats should reflect the error
                assert explorer.stats["errors"] > 0

    def test_keyboard_interrupt_handling(self, temp_output_file):
        """Test that the explorer handles keyboard interrupts gracefully."""
        # Create a mock client that raises KeyboardInterrupt
        mock_client = MagicMock()
        mock_client.collections.side_effect = KeyboardInterrupt()
        mock_client.project = "interrupted-project"

        with patch("main.firestore.Client", return_value=mock_client):
            with patch("main.load_dotenv"):
                with patch("sys.argv", ["main.py", "--output", temp_output_file]):
                    with patch.object(console, "print"):  # Silence console output
                        # Should exit with code 130 (standard for SIGINT)
                        assert main() == 130

    def test_performance_with_large_data(self, temp_output_file):
        """Test performance with simulated large data sets."""
        # Create a mock client with many collections
        mock_client = MagicMock()
        mock_client.project = "large-project"

        # Create 100 mock collections
        mock_collections = []
        for i in range(100):
            collection = MagicMock()
            collection.id = f"collection_{i}"

            # Each collection has 10 empty documents
            docs = []
            for j in range(10):
                doc = MagicMock()
                doc.id = f"doc_{j}"
                doc.to_dict.return_value = {"field": "value"}
                doc.reference.collections.return_value = []
                docs.append(doc)

            collection.stream.return_value = docs
            collection.limit.return_value = collection
            mock_collections.append(collection)

        mock_client.collections.return_value = mock_collections

        # Create a function to return collections by name
        def get_collection(name):
            for col in mock_collections:
                if col.id == name:
                    return col
            return MagicMock()

        mock_client.collection.side_effect = get_collection

        with patch("main.firestore.Client", return_value=mock_client):
            with patch("main.load_dotenv"):
                # Create explorer with lower limits to make test faster
                explorer = FirestoreSchemaExplorer(max_docs=3, max_depth=2, timeout=10)

                # This should complete within a reasonable time
                start_time = time.time()
                schema_doc = explorer.explore_database()
                duration = time.time() - start_time

                # Should process within a reasonable time
                assert duration < 30, f"Exploration took too long: {duration} seconds"

                # Should contain some collection data
                assert "collection_0" in schema_doc

                # Stats should reflect processed data
                assert explorer.stats["collections"] > 0
                assert explorer.stats["documents"] > 0
