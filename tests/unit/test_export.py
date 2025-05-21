# ABOUTME: Tests the export functionality for schema documentation.
# ABOUTME: Validates different output formats and error handling.

import os
import pytest
import json
import tempfile
from unittest.mock import patch
from pathlib import Path

import sys

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from main import FirestoreSchemaExplorer


class TestExportFunctionality:
    """Test the schema export functionality."""

    @pytest.fixture
    def explorer(self, mock_firestore_client):
        """Create a FirestoreSchemaExplorer instance for testing."""
        with patch("main.firestore.Client", return_value=mock_firestore_client):
            with patch("main.load_dotenv"):
                return FirestoreSchemaExplorer()

    @pytest.fixture
    def sample_output(self):
        """Generate a sample schema output for testing export."""
        return """# 🔥 Firestore Schema Explorer

Generated on 2023-08-15 10:00:00
Project: `test-project`

### Collection: `users`
#### Document: `user1`
- `name` (string)
- `age` (integer)
- `active` (boolean)
- `profile` (map)
    - `bio` (string)
    - `joined` (timestamp)

### Collection: `posts`
#### Document: `post1`
- `title` (string)
- `content` (string)
- `author` (reference→users/user1)
- `tags` (array<string>)

## Statistics
- Collections: 2
- Documents sampled: 2
- Fields analyzed: 9
- Duration: 0.25 seconds"""

    def test_markdown_export(self, explorer, sample_output):
        """Test exporting to markdown format."""
        # Create a temporary file path
        with tempfile.NamedTemporaryFile(delete=False, suffix=".md") as temp_file:
            temp_path = temp_file.name

        try:
            # Export the sample output
            result_path = explorer.export_to_file(sample_output, temp_path, format="md")

            # Verify the file was created and has the correct content
            assert os.path.exists(temp_path)
            with open(temp_path, "r", encoding="utf-8") as f:
                content = f.read()
                assert content == sample_output

            # Verify the returned path
            assert result_path == temp_path

        finally:
            # Clean up
            if os.path.exists(temp_path):
                os.unlink(temp_path)

    def test_json_export(self, explorer, sample_output):
        """Test exporting to JSON format."""
        # Create a temporary file path
        with tempfile.NamedTemporaryFile(delete=False, suffix=".json") as temp_file:
            temp_path = temp_file.name

        try:
            # Export the sample output
            result_path = explorer.export_to_file(
                sample_output, temp_path, format="json"
            )

            # Verify the file was created
            assert os.path.exists(temp_path)

            # Verify the JSON structure
            with open(temp_path, "r", encoding="utf-8") as f:
                data = json.load(f)

                # Check expected structure
                assert "users" in data
                assert "posts" in data
                assert "documents" in data["users"]
                assert "user1" in data["users"]["documents"]
                assert "fields" in data["users"]["documents"]["user1"]

                # Check some field content
                user_fields = data["users"]["documents"]["user1"]["fields"]
                assert any(
                    field["name"] == "name" and field["type"] == "string"
                    for field in user_fields
                )

            # Verify the returned path
            assert result_path == temp_path

        finally:
            # Clean up
            if os.path.exists(temp_path):
                os.unlink(temp_path)

    def test_create_missing_directories(self, explorer, sample_output):
        """Test creating missing parent directories when exporting."""
        # Create a temporary directory
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a path with nested directories
            nested_path = os.path.join(temp_dir, "nested", "dirs", "output.md")

            # Export the sample output
            result_path = explorer.export_to_file(sample_output, nested_path)

            # Verify the file was created with the nested directories
            assert os.path.exists(nested_path)

            # Verify the returned path
            assert result_path == nested_path

    def test_export_error_handling(self, explorer, sample_output):
        """Test error handling during export."""
        # Create an invalid path that should cause an error
        invalid_path = "/nonexistent/directory/with/permission/issues/output.md"

        # Mock mkdir to raise an error
        with patch("main.Path.mkdir", side_effect=PermissionError("Permission denied")):
            with pytest.raises(Exception) as excinfo:
                explorer.export_to_file(sample_output, invalid_path)

            # Verify the error message
            assert "Permission denied" in str(
                excinfo.value
            ) or "Failed to export schema" in str(excinfo.value)
