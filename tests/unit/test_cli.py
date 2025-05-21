# ABOUTME: Tests the command-line interface functionality.
# ABOUTME: Validates argument parsing and main function execution.

import sys
import pytest
from unittest.mock import patch, MagicMock
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from main import main


class TestCommandLineInterface:
    """Test the command-line interface functionality."""

    def test_default_arguments(self):
        """Test main function with default arguments."""
        with patch("argparse.ArgumentParser.parse_args") as mock_args:
            # Set up mock arguments with defaults
            mock_args.return_value = MagicMock(
                project_id=None,
                credentials=None,
                max_docs=5,
                depth=5,
                output=None,  # Should use default based on project name
                format="md",
                no_stats=False,
                timeout=30,
                verbose=False,
            )

            with patch("main.FirestoreSchemaExplorer") as mock_explorer_class:
                # Set up the mock explorer instance
                mock_explorer = mock_explorer_class.return_value
                mock_explorer.explore_database.return_value = "Mock schema output"
                mock_explorer.export_to_file.return_value = "output.md"
                # Set up project name before calling main
                mock_explorer.db = MagicMock()
                mock_explorer.db.project = "test-project"

                with patch("main.console.print"):
                    # Call the main function
                    result = main()

                    # Verify FirestoreSchemaExplorer was initialized with correct params
                    mock_explorer_class.assert_called_with(
                        project_id=None,
                        credentials_path=None,
                        max_docs=5,
                        max_depth=5,
                        include_stats=True,
                        timeout=30,
                    )

                    # Verify explore_database was called
                    mock_explorer.explore_database.assert_called_once()

                    # Verify export_to_file was called with project name
                    mock_explorer.export_to_file.assert_called_with(
                        "Mock schema output", "test-project.schema.md", "md"
                    )

                    # Verify exit code is 0 (success)
                    assert result == 0

    def test_custom_arguments(self):
        """Test main function with custom arguments."""
        with patch("argparse.ArgumentParser.parse_args") as mock_args:
            # Set up mock arguments with custom values
            mock_args.return_value = MagicMock(
                project_id="custom-project",
                credentials="/path/to/credentials.json",
                max_docs=10,
                depth=3,
                output="custom_output.json",
                format="json",
                no_stats=True,
                timeout=30,
                verbose=True,
            )

            with patch("main.FirestoreSchemaExplorer") as mock_explorer_class:
                # Set up the mock explorer instance
                mock_explorer = mock_explorer_class.return_value
                mock_explorer.explore_database.return_value = "Mock schema output"
                mock_explorer.export_to_file.return_value = "custom_output.json"

                with patch("main.console.print"):
                    with patch("main.logger") as mock_logger:
                        # Call the main function
                        result = main()

                        # Verify log level was set to DEBUG
                        mock_logger.setLevel.assert_called_with(
                            pytest.approx(10)
                        )  # DEBUG level

                        # Verify FirestoreSchemaExplorer was initialized with correct params
                        mock_explorer_class.assert_called_with(
                            project_id="custom-project",
                            credentials_path="/path/to/credentials.json",
                            max_docs=10,
                            max_depth=3,
                            include_stats=False,
                            timeout=30,
                        )

                        # Verify export_to_file was called with correct params
                        mock_explorer.export_to_file.assert_called_with(
                            "Mock schema output", "custom_output.json", "json"
                        )

                        # Verify exit code is 0 (success)
                        assert result == 0

    def test_error_handling(self):
        """Test main function error handling."""
        with patch("argparse.ArgumentParser.parse_args") as mock_args:
            # Set up mock arguments
            mock_args.return_value = MagicMock(
                project_id=None,
                credentials=None,
                max_docs=5,
                depth=5,
                output="output.md",
                format="md",
                no_stats=False,
                timeout=30,
                verbose=False,
            )

            with patch("main.FirestoreSchemaExplorer") as mock_explorer_class:
                # Set up the explorer to raise an exception
                mock_explorer_class.side_effect = Exception("Test error")

                with patch("main.console.print") as mock_print:
                    with patch("main.console.print_exception") as mock_print_exception:
                        # Call the main function
                        result = main()

                        # Verify error was printed
                        mock_print.assert_called_with("[bold red]Error:[/] Test error")

                        # Verify exception trace was not printed (verbose=False)
                        mock_print_exception.assert_not_called()

                        # Verify exit code is 1 (error)
                        assert result == 1

    def test_verbose_error_handling(self):
        """Test verbose error handling in main function."""
        with patch("argparse.ArgumentParser.parse_args") as mock_args:
            # Set up mock arguments with verbose=True
            mock_args.return_value = MagicMock(
                project_id=None,
                credentials=None,
                max_docs=5,
                depth=5,
                output="output.md",
                format="md",
                no_stats=False,
                timeout=30,
                verbose=True,
            )

            with patch("main.FirestoreSchemaExplorer") as mock_explorer_class:
                # Set up the explorer to raise an exception
                mock_explorer_class.side_effect = Exception("Test error")

                with patch("main.console.print"):
                    with patch("main.console.print_exception") as mock_print_exception:
                        # Call the main function
                        result = main()

                        # Verify exception trace was printed (verbose=True)
                        mock_print_exception.assert_called_once()

                        # Verify exit code is 1 (error)
                        assert result == 1
