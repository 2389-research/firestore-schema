# ABOUTME: Tests the initialization of the FirestoreSchemaExplorer class.
# ABOUTME: Validates constructor parameters and error handling.

import os
import pytest
from unittest.mock import patch
from pathlib import Path

import sys

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from main import FirestoreSchemaExplorer


class TestFirestoreSchemaExplorerInit:
    """Test the FirestoreSchemaExplorer class initialization."""

    def test_default_init(self, mock_firestore_client):
        """Test initialization with default parameters."""
        with patch("main.firestore.Client", return_value=mock_firestore_client):
            with patch("main.load_dotenv"):
                explorer = FirestoreSchemaExplorer()

                assert explorer.max_docs == 5
                assert explorer.max_depth == 5
                assert explorer.include_stats is True
                assert explorer.sample_arrays is True
                assert explorer.array_sample_size == 3
                assert explorer.db is mock_firestore_client
                assert explorer.stats["collections"] == 0
                assert explorer.stats["documents"] == 0
                assert explorer.stats["fields"] == 0
                assert "start_time" in explorer.stats

    def test_custom_parameters(self, mock_firestore_client):
        """Test initialization with custom parameters."""
        with patch("main.firestore.Client", return_value=mock_firestore_client):
            with patch("main.load_dotenv"):
                explorer = FirestoreSchemaExplorer(
                    project_id="test-project",
                    max_docs=10,
                    max_depth=3,
                    include_stats=False,
                    sample_arrays=False,
                    array_sample_size=5,
                )

                assert explorer.max_docs == 10
                assert explorer.max_depth == 3
                assert explorer.include_stats is False
                assert explorer.sample_arrays is False
                assert explorer.array_sample_size == 5
                assert explorer.db is mock_firestore_client

    def test_credentials_path(self, mock_firestore_client):
        """Test initialization with credentials path."""
        with patch("main.firestore.Client", return_value=mock_firestore_client):
            with patch("main.load_dotenv"):
                with patch("main.Path.is_file", return_value=True):
                    with patch("main.os") as mock_os:
                        FirestoreSchemaExplorer(
                            credentials_path="/fake/path/credentials.json"
                        )

                        # Check that the environment variable was set
                        mock_os.environ.__setitem__.assert_called_with(
                            "GOOGLE_APPLICATION_CREDENTIALS",
                            str(
                                Path("/fake/path/credentials.json")
                                .expanduser()
                                .resolve()
                            ),
                        )

    def test_missing_credentials_file(self):
        """Test error handling when credentials file doesn't exist."""
        with patch("main.load_dotenv"):
            with patch("main.Path.is_file", return_value=False):
                with pytest.raises(FileNotFoundError) as excinfo:
                    FirestoreSchemaExplorer(
                        credentials_path="/fake/path/nonexistent.json"
                    )

                assert "Credentials file not found" in str(excinfo.value)

    def test_firestore_client_error(self):
        """Test error handling when Firestore client fails to initialize."""
        with patch("main.load_dotenv"):
            with patch(
                "main.firestore.Client", side_effect=Exception("Connection error")
            ):
                with pytest.raises(Exception) as excinfo:
                    FirestoreSchemaExplorer()

                assert "Connection error" in str(excinfo.value)

    def test_use_env_credentials(self, mock_firestore_client):
        """Test using credentials from environment variable."""
        with patch("main.firestore.Client", return_value=mock_firestore_client):
            with patch("main.load_dotenv"):
                with patch("main.Path.is_file", return_value=True):
                    with patch.dict(
                        os.environ,
                        {"GOOGLE_APPLICATION_CREDENTIALS": "/env/path/creds.json"},
                    ):
                        explorer = FirestoreSchemaExplorer()
                        # Should use existing environment variable, not overwrite it
                        assert explorer.db is mock_firestore_client
