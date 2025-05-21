# ABOUTME: Tests the field description functionality.
# ABOUTME: Validates correct description of document fields and nested structures.

import pytest
from unittest.mock import patch
from pathlib import Path

import sys

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from main import FirestoreSchemaExplorer


class TestFieldDescription:
    """Test the field description functionality."""

    @pytest.fixture
    def explorer(self, mock_firestore_client):
        """Create a FirestoreSchemaExplorer instance for testing."""
        with patch("main.firestore.Client", return_value=mock_firestore_client):
            with patch("main.load_dotenv"):
                return FirestoreSchemaExplorer()

    def test_simple_fields(self, explorer, mock_document_data):
        """Test description of simple fields."""
        # Create a simple document with basic fields
        simple_data = {"string_field": "test", "int_field": 42, "bool_field": True}

        output_lines = []
        explorer.describe_fields(simple_data, 0, output_lines)

        assert len(output_lines) == 3
        assert "- `bool_field` (boolean)" in output_lines
        assert "- `int_field` (integer)" in output_lines
        assert "- `string_field` (string)" in output_lines

        # Check that fields were counted
        assert explorer.stats["fields"] == 3

    def test_nested_fields(self, explorer):
        """Test description of nested fields with proper indentation."""
        nested_data = {
            "top_level": "value",
            "nested_map": {"level1_field": "nested value", "another_field": 42},
        }

        output_lines = []
        explorer.describe_fields(nested_data, 0, output_lines)

        assert len(output_lines) == 4
        assert "- `top_level` (string)" in output_lines
        assert "- `nested_map` (map)" in output_lines
        assert "  - `level1_field` (string)" in output_lines
        assert "  - `another_field` (integer)" in output_lines

        # Check that all fields were counted (2 top-level + 2 nested)
        assert explorer.stats["fields"] == 4

    def test_deep_nesting(self, explorer):
        """Test description of deeply nested fields."""
        deep_data = {"level1": {"level2": {"level3": {"deep_field": "value"}}}}

        output_lines = []
        explorer.describe_fields(deep_data, 0, output_lines)

        # Should have 4 lines (one for each level)
        assert len(output_lines) == 4
        assert "- `level1` (map)" in output_lines
        assert "  - `level2` (map)" in output_lines
        assert "    - `level3` (map)" in output_lines
        assert "      - `deep_field` (string)" in output_lines

        # Check stats
        assert explorer.stats["fields"] == 4

    def test_empty_data(self, explorer):
        """Test description of empty document."""
        empty_data = {}

        output_lines = []
        explorer.describe_fields(empty_data, 0, output_lines)

        assert len(output_lines) == 0
        assert explorer.stats["fields"] == 0
