#!/usr/bin/env python3
"""
FirestoreSchemaExplorer: A robust tool for exploring and documenting Firestore database schemas.

Features:
- Auto-detects schema from existing documents
- Rich markdown output with type information
- Support for all Firestore data types
- Customizable exploration depth and document sampling
- Error handling and logging
- Export options (markdown, JSON)
- Progress tracking for large databases
"""

import os
import sys
import json
import time
import argparse
import logging
import concurrent.futures
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from pathlib import Path
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskID
from rich.panel import Panel
from rich.markdown import Markdown
from rich.logging import RichHandler
from dotenv import load_dotenv
from google.cloud import firestore
from google.api_core.exceptions import (
    PermissionDenied,
)

# Configure rich console
console = Console()

# Set up logging with rich
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler(rich_tracebacks=True, console=console)],
)
logger = logging.getLogger("firestore_schema")


class TimeoutError(Exception):
    """Custom exception for operation timeouts."""

    pass


class FirestoreSchemaExplorer:
    """Explore and document Firestore database schema."""

    # Type mapping for Firestore data types
    TYPE_MAPPING = {
        str: "string",
        bool: "boolean",
        int: "integer",
        float: "float",
        dict: "map",
        list: "array",
        type(None): "null",
        datetime: "timestamp",
        firestore.GeoPoint: "geopoint",
        bytes: "bytes",
    }

    def __init__(
        self,
        project_id: Optional[str] = None,
        credentials_path: Optional[str] = None,
        max_docs: int = 5,
        max_depth: int = 5,
        include_stats: bool = True,
        sample_arrays: bool = True,
        array_sample_size: int = 3,
        timeout: int = 30,  # Default timeout in seconds
    ):
        """Initialize the FirestoreSchemaExplorer.

        Args:
            project_id: Google Cloud project ID (optional if in env vars)
            credentials_path: Path to service account credentials JSON file
            max_docs: Maximum number of documents to sample per collection
            max_depth: Maximum depth to traverse subcollections
            include_stats: Include statistics like document counts
            sample_arrays: Sample array contents to determine element types
            array_sample_size: Number of array elements to sample
        """
        self.max_docs = max_docs
        self.max_depth = max_depth
        self.include_stats = include_stats
        self.sample_arrays = sample_arrays
        self.array_sample_size = array_sample_size
        self.timeout = timeout
        self.stats = {
            "collections": 0,
            "documents": 0,
            "fields": 0,
            "start_time": time.time(),
            "timeouts": 0,
            "errors": 0,
        }
        self._processed_paths = set()
        self._progress = None
        self._main_task_id = None
        self._collection_tree = {}

        # Initialize Firestore client
        self._init_firestore_client(project_id, credentials_path)

    def _run_with_timeout(
        self, func, *args, **kwargs
    ) -> Tuple[Any, bool, Optional[Exception]]:
        """Run a function with a timeout.

        Args:
            func: The function to run
            *args: Arguments to pass to the function
            **kwargs: Keyword arguments to pass to the function

        Returns:
            Tuple of (result, timed_out, exception)
            - result: The result of the function if successful, None otherwise
            - timed_out: Boolean indicating whether the operation timed out
            - exception: Exception raised by the function, if any
        """
        # Use ThreadPoolExecutor for timeout management
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(func, *args, **kwargs)
            try:
                result = future.result(timeout=self.timeout)
                return result, False, None
            except concurrent.futures.TimeoutError:
                self.stats["timeouts"] += 1
                logger.warning(f"Operation timed out after {self.timeout} seconds")
                return (
                    None,
                    True,
                    TimeoutError(f"Operation timed out after {self.timeout} seconds"),
                )
            except Exception as e:
                self.stats["errors"] += 1
                logger.warning(f"Operation failed: {str(e)}")
                return None, False, e

    def _init_firestore_client(
        self, project_id: Optional[str], credentials_path: Optional[str]
    ):
        """Initialize the Firestore client with proper error handling."""
        # Load environment variables
        load_dotenv()

        # Use provided credentials path or get from environment
        cred_path = credentials_path or os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

        if cred_path:
            # Validate path exists
            cred_path = Path(cred_path).expanduser().resolve()
            if not cred_path.is_file():
                raise FileNotFoundError(f"Credentials file not found: {cred_path}")
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(cred_path)
            logger.info(f"Using credentials from: {cred_path}")

        try:
            # Initialize client with optional project_id
            client_kwargs = {}
            if project_id:
                client_kwargs["project"] = project_id
            self.db = firestore.Client(**client_kwargs)
            logger.info(f"Connected to Firestore in project: {self.db.project}")
        except Exception as e:
            logger.error(f"Failed to initialize Firestore client: {e}")
            raise

    def indent(self, level: int) -> str:
        """Return an indentation string based on the nesting level."""
        return "  " * level

    def describe_type(self, value: Any) -> str:
        """Get the descriptive type name of a value."""
        # Handle None explicitly
        if value is None:
            return "null"

        # Check for Firestore specific types
        if hasattr(value, "timestamp"):
            return "timestamp"
        elif hasattr(value, "longitude") and hasattr(value, "latitude"):
            return "geopoint"
        elif hasattr(value, "path"):
            return f"reference→{value.path}"

        # Basic Python types
        value_type = type(value)
        if value_type in self.TYPE_MAPPING:
            base_type = self.TYPE_MAPPING[value_type]

            # Special handling for collections
            if isinstance(value, list):
                if not value or len(value) == 0:
                    return "array<?>"

                # Sample array elements to detect type
                if self.sample_arrays:
                    sample_size = min(len(value), self.array_sample_size)
                    element_types = set()
                    for i in range(sample_size):
                        element_types.add(self.describe_type(value[i]))

                    if len(element_types) == 1:
                        return f"array<{next(iter(element_types))}>"
                    else:
                        return f"array<mixed:{','.join(sorted(element_types))}>"
                else:
                    return f"array<{self.describe_type(value[0])}>"

            return base_type

        # Fallback for unknown types
        return value_type.__name__

    def describe_fields(
        self, data: Dict[str, Any], level: int, output_lines: List[str]
    ):
        """Recursively describe fields of a document.

        Args:
            data: Document data dictionary
            level: Current nesting level
            output_lines: List to append output lines to
        """
        for key, value in sorted(data.items()):
            field_type = self.describe_type(value)
            # For proper markdown bullets, indent with 2 spaces per level
            indent = " " * (2 * level)
            output_lines.append(f"{indent}- `{key}` ({field_type})")
            self.stats["fields"] += 1

            # Recurse into maps/dictionaries
            if isinstance(value, dict):
                self.describe_fields(value, level + 1, output_lines)

    def _safe_stream_collection(self, collection, limit=None) -> List[Any]:
        """Safely stream a collection with timeout handling.

        Args:
            collection: The collection reference to stream
            limit: Optional document limit

        Returns:
            List of documents or empty list on error/timeout
        """
        # Apply limit if specified
        query = collection.limit(limit) if limit is not None else collection

        # Define the streaming function
        def stream_docs():
            return list(query.stream())

        # Run with timeout
        docs, timed_out, error = self._run_with_timeout(stream_docs)

        if timed_out:
            logger.warning(f"Streaming collection {collection.id} timed out")
            return []

        if error:
            logger.warning(f"Error streaming collection {collection.id}: {error}")
            if isinstance(error, PermissionDenied):
                raise error  # Re-raise permission errors for special handling
            return []

        return docs or []

    def process_collection(
        self, path: str, level: int = 0, parent_task_id: Optional[TaskID] = None
    ) -> List[str]:
        """Process a collection and its documents.

        Args:
            path: Collection path
            level: Current nesting level
            parent_task_id: Parent progress task ID

        Returns:
            List of output lines
        """
        # Guard against cycles and excessive recursion
        if path in self._processed_paths or level >= self.max_depth:
            return []

        self._processed_paths.add(path)
        output_lines = []

        try:
            collection = self.db.collection(path)
            self.stats["collections"] += 1

            # Create progress task for this collection
            task_description = f"Collection: {path}"
            collection_task_id = None
            if self._progress:
                collection_task_id = self._progress.add_task(
                    task_description, total=self.max_docs, parent=parent_task_id
                )

            # Get collection stats if requested
            doc_count = None
            if self.include_stats:
                try:
                    # Efficiently count documents (limited to 1000 for performance)
                    query = collection.limit(1000)
                    docs, timed_out, error = self._run_with_timeout(
                        lambda: list(query.stream())
                    )

                    if timed_out:
                        doc_count_str = "unknown (timed out)"
                    elif error:
                        logger.warning(f"Failed to count documents in {path}: {error}")
                        doc_count_str = "unknown (error)"
                    else:
                        doc_count = len(docs)
                        doc_count_str = (
                            f"{doc_count}+" if doc_count >= 1000 else str(doc_count)
                        )
                except Exception as e:
                    logger.warning(f"Failed to count documents in {path}: {e}")
                    doc_count_str = "unknown"

            # Track in collection tree
            actual_doc_count = doc_count if doc_count is not None else 0
            self._collection_tree[path] = {
                'doc_count': actual_doc_count,
                'doc_count_str': doc_count_str if doc_count is not None else "unknown"
            }

            # Add collection header
            collection_header = f"### Collection: `{path}`"
            if doc_count is not None:
                collection_header += f" ({doc_count_str} documents)"
            output_lines.append(collection_header)

            # Sample documents
            try:
                try:
                    docs = self._safe_stream_collection(collection, self.max_docs)
                except PermissionDenied:
                    # Handle permission denied error
                    output_lines.append(
                        f"{self.indent(level+1)}*Permission denied when accessing documents*"
                    )
                    return output_lines

                if not docs:
                    output_lines.append("*No documents found*")
                    return output_lines

                # Process each document
                for i, doc in enumerate(docs):
                    if self._progress and collection_task_id:
                        self._progress.update(collection_task_id, completed=i + 1)

                    doc_data = doc.to_dict()
                    if not doc_data:
                        continue

                    self.stats["documents"] += 1
                    output_lines.append(f"#### Document: `{doc.id}`")
                    self.describe_fields(doc_data, level + 2, output_lines)

                    # Process subcollections if not at max depth
                    if level < self.max_depth - 1:
                        # Get subcollections with timeout handling
                        subcollections, timed_out, error = self._run_with_timeout(
                            lambda: list(doc.reference.collections())
                        )

                        if timed_out:
                            output_lines.append(
                                "*Timed out when fetching subcollections*"
                            )
                            continue

                        if error:
                            output_lines.append(
                                f"*Error fetching subcollections: {error}*"
                            )
                            continue

                        for subcol in subcollections or []:
                            # Construct the full subcollection path
                            subcol_path = f"{doc.reference.path}/{subcol.id}"
                            subcol_output = self.process_collection(
                                subcol_path, level + 2, collection_task_id
                            )
                            output_lines.extend(subcol_output)

            except PermissionDenied:
                output_lines.append("*Permission denied when accessing documents*")
            except TimeoutError as e:
                output_lines.append(f"*Timed out when accessing documents: {str(e)}*")
                logger.warning(f"Timeout processing documents in {path}: {e}")
            except Exception as e:
                output_lines.append(f"*Error accessing documents: {str(e)}*")
                logger.error(f"Error processing documents in {path}: {e}")
                self.stats["errors"] += 1

        except TimeoutError as e:
            output_lines.append(f"### `{path}`: *Timeout: {str(e)}*")
            logger.warning(f"Timeout processing collection {path}: {e}")
        except Exception as e:
            output_lines.append(f"### `{path}`: *Error: {str(e)}*")
            logger.error(f"Error processing collection {path}: {e}")
            self.stats["errors"] += 1

        return output_lines

    def generate_ascii_tree(self) -> str:
        """Generate an ASCII tree representation of the database structure.
        
        Returns:
            String containing the ASCII tree
        """
        if not self._collection_tree:
            return ""
            
        output = [f"Project: `{self.db.project}`"]
        
        # Separate top-level collections and subcollections
        top_level = {}
        subcollections = {}
        
        for path, info in self._collection_tree.items():
            path_parts = path.split('/')
            if len(path_parts) == 1:
                # Top-level collection
                top_level[path] = info
            else:
                # Subcollection: collection/doc_id/subcollection
                parent_collection = path_parts[0]
                doc_id = path_parts[1]
                subcol_name = path_parts[2]
                
                if parent_collection not in subcollections:
                    subcollections[parent_collection] = {}
                if doc_id not in subcollections[parent_collection]:
                    subcollections[parent_collection][doc_id] = []
                    
                subcollections[parent_collection][doc_id].append((subcol_name, info))
        
        # Generate tree output
        sorted_top_level = sorted(top_level.items())
        
        for i, (collection_name, collection_info) in enumerate(sorted_top_level):
            is_last_collection = i == len(sorted_top_level) - 1
            prefix = "└── " if is_last_collection else "├── "
            doc_count = collection_info.get('doc_count', 0)
            output.append(f"{prefix}{collection_name} ({doc_count} docs)")
            
            # Add subcollections if they exist
            if collection_name in subcollections:
                sorted_docs = sorted(subcollections[collection_name].items())
                
                for j, (doc_id, doc_subcols) in enumerate(sorted_docs):
                    is_last_doc = j == len(sorted_docs) - 1
                    continuation = "    " if is_last_collection else "│   "
                    doc_prefix = "└── " if is_last_doc else "├── "
                    
                    output.append(f"{continuation}{doc_prefix}{doc_id}/")
                    
                    for k, (subcol_name, subcol_info) in enumerate(doc_subcols):
                        is_last_subcol = k == len(doc_subcols) - 1
                        subcol_continuation = continuation + ("    " if is_last_doc else "│   ")
                        subcol_prefix = "└── " if is_last_subcol else "├── "
                        subcol_doc_count = subcol_info.get('doc_count', 0)
                        output.append(f"{subcol_continuation}{subcol_prefix}{subcol_name} ({subcol_doc_count} docs)")
        
        return "\n".join(output)

    def explore_database(self) -> str:
        """Explore the entire Firestore database.

        Returns:
            Markdown string containing the schema documentation
        """
        output = ["# 🔥 Firestore Schema Explorer"]
        output.append(f"\nGenerated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        output.append(f"Project: `{self.db.project}`\n")

        try:
            # Use rich progress display
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                console=console,
            ) as progress:
                self._progress = progress
                self._main_task_id = progress.add_task(
                    "Exploring Firestore schema...", total=None
                )

                # Get all top-level collections
                try:
                    # Get collections with timeout handling
                    collections, timed_out, error = self._run_with_timeout(
                        lambda: list(self.db.collections())
                    )

                    if timed_out:
                        output.append("*Timed out while listing collections.*")
                        return "\n".join(output)

                    if error:
                        output.append(f"*Error listing collections: {str(error)}*")
                        logger.error(f"Failed to list collections: {error}")
                        return "\n".join(output)

                    if not collections:
                        output.append("*No collections found in the database.*")
                        return "\n".join(output)

                    progress.update(self._main_task_id, total=len(collections))

                    # Process each collection
                    for i, col in enumerate(collections):
                        progress.update(
                            self._main_task_id,
                            description=f"Processing collection {i+1}/{len(collections)}: {col.id}",
                            completed=i,
                        )
                        collection_output = self.process_collection(
                            col.id, parent_task_id=self._main_task_id
                        )
                        output.extend(collection_output)

                    progress.update(self._main_task_id, completed=len(collections))
                    
                    # Add ASCII tree overview after exploration
                    tree = self.generate_ascii_tree()
                    if tree:
                        # Insert tree at the beginning after header
                        tree_lines = ["\n## Database Structure", "```", tree, "```", ""]
                        # Find where to insert (after project line)
                        insert_index = 3  # After header, generated date, and project line
                        for i, line in enumerate(tree_lines):
                            output.insert(insert_index + i, line)

                except TimeoutError as e:
                    output.append(f"*Timeout listing collections: {str(e)}*")
                    logger.warning(f"Timeout listing collections: {e}")
                    self.stats["timeouts"] += 1
                except Exception as e:
                    output.append(f"*Error listing collections: {str(e)}*")
                    logger.error(f"Failed to list collections: {e}")
                    self.stats["errors"] += 1

        except TimeoutError as e:
            output.append(f"*Exploration timeout: {str(e)}*")
            logger.warning(f"Exploration timeout: {e}")
            self.stats["timeouts"] += 1
        except Exception as e:
            output.append(f"*Exploration error: {str(e)}*")
            logger.error(f"Exploration error: {e}")
            self.stats["errors"] += 1

        # Add statistics
        if self.include_stats:
            self.stats["duration"] = time.time() - self.stats["start_time"]
            output.append("\n## Statistics")
            output.append(f"- Collections: {self.stats['collections']}")
            output.append(f"- Documents sampled: {self.stats['documents']}")
            output.append(f"- Fields analyzed: {self.stats['fields']}")
            output.append(f"- Duration: {self.stats['duration']:.2f} seconds")
            output.append(f"- Timeouts: {self.stats['timeouts']}")
            output.append(f"- Errors: {self.stats['errors']}")

        return "\n".join(output)

    def export_to_file(self, output: str, filename: str, format: str = "md"):
        """Export the schema documentation to a file.

        Args:
            output: Schema documentation string
            filename: Output filename
            format: Output format ('md' or 'json')
        """
        try:
            output_path = Path(filename)

            # Create parent directories if they don't exist
            output_path.parent.mkdir(parents=True, exist_ok=True)

            if format.lower() == "json":
                # Convert markdown to a structured JSON format
                # This is a simplified implementation
                collections_data = {}
                current_collection = None
                current_document = None

                for line in output.split("\n"):
                    if "### Collection: `" in line:
                        collection_name = line.split("`")[1]
                        collections_data[collection_name] = {"documents": {}}
                        current_collection = collection_name
                    elif "#### Document: `" in line and current_collection:
                        doc_id = line.split("`")[1]
                        collections_data[current_collection]["documents"][doc_id] = {
                            "fields": []
                        }
                        current_document = doc_id
                    elif "- `" in line and current_collection and current_document:
                        field_info = line.strip().split("`")[1]
                        field_type = line.split("(")[1].split(")")[0]
                        collections_data[current_collection]["documents"][
                            current_document
                        ]["fields"].append({"name": field_info, "type": field_type})

                with open(output_path, "w", encoding="utf-8") as f:
                    json.dump(collections_data, f, indent=2)
            else:
                # Write markdown directly
                with open(output_path, "w", encoding="utf-8") as f:
                    f.write(output)

            logger.info(f"Schema exported to {output_path}")
            return str(output_path)

        except Exception as e:
            logger.error(f"Failed to export schema: {e}")
            raise


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description="Explore and document Firestore database schema",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "--project-id", "-p", help="Google Cloud project ID (defaults to environment)"
    )

    parser.add_argument(
        "--credentials", "-c", help="Path to service account credentials JSON file"
    )

    parser.add_argument(
        "--max-docs",
        "-m",
        type=int,
        default=5,
        help="Maximum number of documents to sample per collection",
    )

    parser.add_argument(
        "--depth",
        "-d",
        type=int,
        default=5,
        help="Maximum depth to explore subcollections",
    )

    parser.add_argument(
        "--output",
        "-o",
        default=None,
        help="Output file path (default: <project_id>.schema.md)",
    )

    parser.add_argument(
        "--format",
        "-f",
        choices=["md", "json"],
        default="md",
        help="Output format (markdown or JSON)",
    )

    parser.add_argument(
        "--no-stats", action="store_true", help="Don't include statistics in the output"
    )

    parser.add_argument(
        "--timeout",
        "-t",
        type=int,
        default=30,
        help="Timeout in seconds for Firestore operations",
    )

    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable verbose logging"
    )

    args = parser.parse_args()

    # Set log level
    if args.verbose:
        logger.setLevel(logging.DEBUG)

    try:
        # Create explorer
        explorer = FirestoreSchemaExplorer(
            project_id=args.project_id,
            credentials_path=args.credentials,
            max_docs=args.max_docs,
            max_depth=args.depth,
            include_stats=not args.no_stats,
            timeout=args.timeout,
        )

        # Run exploration
        console.print("🔍 Exploring Firestore schema...", style="bold blue")
        schema_doc = explorer.explore_database()

        # Determine output path
        output_file = args.output
        if output_file is None:
            # Default to project_name.schema.md
            project_id = explorer.db.project
            output_file = f"{project_id}.schema.md"

        # Export results
        output_path = explorer.export_to_file(schema_doc, output_file, args.format)

        # Show preview in terminal
        console.print("\n[bold green]✅ Schema exploration complete![/]")
        console.print(f"Output saved to: [bold]{output_path}[/]")

        if args.format == "md":
            console.print("\n[bold]Schema Preview:[/]")
            console.print(
                Markdown(
                    schema_doc[:2000] + "...\n\n*Preview truncated*"
                    if len(schema_doc) > 2000
                    else schema_doc
                )
            )

    except KeyboardInterrupt:
        console.print("\n[bold yellow]Exploration cancelled by user.[/]")
        return 130
    except TimeoutError as e:
        console.print(
            Panel(
                f"[bold yellow]Operation timed out:[/] {str(e)}",
                title="Timeout",
                expand=False,
            )
        )
        console.print("\nTry increasing the timeout with the --timeout parameter.")
        return 1
    except Exception as e:
        console.print(f"[bold red]Error:[/] {str(e)}")
        if args.verbose:
            console.print_exception()
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
