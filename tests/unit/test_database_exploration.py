# ABOUTME: Tests the full database exploration functionality.
# ABOUTME: Validates end-to-end exploration process with different configurations.

import pytest
import time
from unittest.mock import patch, MagicMock
from pathlib import Path

import sys

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from main import FirestoreSchemaExplorer
from rich.progress import Progress


class TestDatabaseExploration:
    """Test the full database exploration process."""

    @pytest.fixture
    def explorer(self, firestore_with_collections):
        """Create a FirestoreSchemaExplorer instance with mock collections."""
        with patch("main.firestore.Client", return_value=firestore_with_collections):
            with patch("main.load_dotenv"):
                explorer = FirestoreSchemaExplorer()
                # We need to patch the rich progress display for testing
                explorer._progress = None
                return explorer

    def test_explore_database_basic(self, explorer):
        """Test basic database exploration."""
        output = explorer.explore_database()

        # Verify header
        assert "# 🔥 Firestore Schema Explorer" in output
        assert f"Project: `{explorer.db.project}`" in output

        # Verify collections were processed
        assert "### Collection: `empty_collection`" in output
        assert "### Collection: `test_collection`" in output
        assert "### Collection: `secure_collection`" in output

        # Verify stats were included
        assert "## Statistics" in output
        assert "Collections: " in output
        assert "Documents sampled: " in output
        assert "Fields analyzed: " in output
        assert "Duration: " in output

    def test_explore_without_stats(self, explorer):
        """Test exploration without statistics."""
        explorer.include_stats = False
        output = explorer.explore_database()

        # Verify header
        assert "# 🔥 Firestore Schema Explorer" in output

        # Verify collections were processed
        assert "### Collection: `empty_collection`" in output

        # Verify stats were NOT included
        assert "## Statistics" not in output
        assert "Collections: " not in output

    def test_explore_with_progress(self, explorer):
        """Test exploration with progress tracking."""
        # Create a mock progress object
        mock_progress = MagicMock(spec=Progress)
        mock_progress.add_task.return_value = 1

        with patch("main.Progress", return_value=mock_progress):
            with patch.object(mock_progress, "__enter__", return_value=mock_progress):
                with patch.object(mock_progress, "__exit__", return_value=None):
                    explorer._progress = mock_progress
                    explorer.explore_database()

        # Verify progress was tracked - only check that add_task was called, not specific parameters
        assert mock_progress.add_task.called
        assert mock_progress.update.call_count > 0

    def test_explore_empty_database(self, explorer):
        """Test exploring a database with no collections."""
        # Configure firestore to return no collections
        explorer.db.collections.return_value = []

        output = explorer.explore_database()

        # Should indicate no collections found
        assert "# 🔥 Firestore Schema Explorer" in output
        assert "*No collections found in the database.*" in output

    def test_explore_error_handling(self, explorer):
        """Test error handling during exploration."""
        # Make collections() raise an exception
        explorer.db.collections.side_effect = Exception("Database connection error")

        output = explorer.explore_database()

        # Should contain error message
        assert "# 🔥 Firestore Schema Explorer" in output
        assert "*Error listing collections: Database connection error*" in output

    def test_processing_timeout_simulation(self, explorer):
        """Test handling potential long-running operations."""

        # Simulate a very slow document stream operation
        def slow_stream():
            time.sleep(0.01)  # Small delay for testing
            return [MagicMock()]

        explorer.db.collection("test_collection").stream = slow_stream

        # Should complete without hanging
        start_time = time.time()
        explorer.explore_database()
        duration = time.time() - start_time

        # Exploration should complete in a reasonable time
        assert duration < 5  # Very generous timeout for a small test
