# ABOUTME: Tests timeout handling features of the FirestoreSchemaExplorer.
# ABOUTME: Validates that operations time out gracefully rather than hanging.

import time
from unittest.mock import MagicMock, patch

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from main import FirestoreSchemaExplorer, TimeoutError


class TestTimeoutHandling:
    """Test timeout handling features."""

    def test_run_with_timeout_success(self):
        """Test that fast operations complete successfully."""
        with patch("main.firestore.Client"):
            with patch("main.load_dotenv"):
                explorer = FirestoreSchemaExplorer(timeout=5)

        # Define a fast operation
        def fast_operation():
            return "success"

        # Should complete successfully
        result, timed_out, error = explorer._run_with_timeout(fast_operation)

        assert not timed_out
        assert error is None
        assert result == "success"

    def test_run_with_timeout_timeout(self):
        """Test that slow operations time out."""
        with patch("main.firestore.Client"):
            with patch("main.load_dotenv"):
                explorer = FirestoreSchemaExplorer(timeout=1)

        # Define a slow operation that would hang
        def slow_operation():
            time.sleep(10)  # This would normally hang
            return "success"

        # Should time out
        result, timed_out, error = explorer._run_with_timeout(slow_operation)

        assert timed_out
        assert isinstance(error, TimeoutError)
        assert result is None
        assert explorer.stats["timeouts"] == 1

    def test_run_with_timeout_error(self):
        """Test that errors are properly propagated."""
        with patch("main.firestore.Client"):
            with patch("main.load_dotenv"):
                explorer = FirestoreSchemaExplorer(timeout=5)

        # Define an operation that raises an error
        def error_operation():
            raise ValueError("Test error")

        # Should capture the error
        result, timed_out, error = explorer._run_with_timeout(error_operation)

        assert not timed_out
        assert isinstance(error, ValueError)
        assert str(error) == "Test error"
        assert result is None
        assert explorer.stats["errors"] == 1

    def test_safe_stream_collection(self):
        """Test safe collection streaming with timeout handling."""
        with patch("main.firestore.Client"):
            with patch("main.load_dotenv"):
                explorer = FirestoreSchemaExplorer(timeout=1)

        # Create a mock collection that would hang when streamed
        mock_collection = MagicMock()
        mock_collection.id = "hanging_collection"

        def hanging_stream():
            time.sleep(10)  # This would normally hang
            return []

        mock_query = MagicMock()
        mock_query.stream.side_effect = hanging_stream
        mock_collection.limit.return_value = mock_query

        # Should return empty list and record timeout
        result = explorer._safe_stream_collection(mock_collection, 10)

        assert result == []
        assert explorer.stats["timeouts"] == 1

    def test_explore_database_with_timeout(self):
        """Test that database exploration handles timeouts gracefully."""
        with patch("main.firestore.Client"):
            with patch("main.load_dotenv"):
                explorer = FirestoreSchemaExplorer(timeout=1)

        # Create a mock db that times out when listing collections
        mock_db = MagicMock()

        def hanging_collections():
            # Instead of actually sleeping, raise TimeoutError to simulate
            raise TimeoutError("Simulated timeout")

        mock_db.collections.side_effect = hanging_collections
        explorer.db = mock_db

        # Should not hang and should include timeout message
        start_time = time.time()
        result = explorer.explore_database()
        duration = time.time() - start_time

        # Should complete in around timeout value (1 second) plus some overhead
        assert duration < 5
        assert "*Error listing collections: Simulated timeout*" in result
        assert explorer.stats["errors"] >= 1

    def test_subcollection_timeout(self):
        """Test handling of subcollection retrieval timeouts."""
        with patch("main.firestore.Client"):
            with patch("main.load_dotenv"):
                explorer = FirestoreSchemaExplorer(timeout=1)

        # Mock document with a reference that times out when getting subcollections
        mock_doc = MagicMock()
        mock_doc.id = "test_doc"
        mock_doc.to_dict.return_value = {"field": "value"}

        mock_ref = MagicMock()

        def hanging_subcollections():
            # Instead of actually sleeping, raise TimeoutError to simulate
            raise TimeoutError("Simulated timeout")

        mock_ref.collections.side_effect = hanging_subcollections
        mock_doc.reference = mock_ref

        # Mock collection that returns our test document
        mock_collection = MagicMock()
        mock_collection.id = "test_collection"
        mock_collection.stream.return_value = [mock_doc]
        mock_collection.limit.return_value = mock_collection

        # Mock db that returns our test collection
        mock_db = MagicMock()
        mock_db.collection.return_value = mock_collection
        explorer.db = mock_db

        # Process the collection - should not hang on subcollections
        start_time = time.time()
        result = explorer.process_collection("test_collection")
        duration = time.time() - start_time

        # Should complete in around timeout value plus some overhead
        assert duration < 5
        assert any(
            "*Error fetching subcollections: Simulated timeout*" in line
            for line in result
        )
        assert explorer.stats["errors"] >= 1
