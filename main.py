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
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple, Union
from pathlib import Path
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskID
from rich.table import Table
from rich.markdown import Markdown
from rich.logging import RichHandler
from dotenv import load_dotenv
from google.cloud import firestore
from google.cloud.firestore_v1.base_document import DocumentSnapshot
from google.cloud.firestore_v1.collection import CollectionReference
from google.api_core.exceptions import GoogleAPIError, NotFound, PermissionDenied

# Configure rich console
console = Console()

# Set up logging with rich
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    datefmt="[%X]",
    handlers=[RichHandler(rich_tracebacks=True, console=console)]
)
logger = logging.getLogger("firestore_schema")

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
        self.stats = {
            "collections": 0,
            "documents": 0,
            "fields": 0,
            "start_time": time.time()
        }
        self._processed_paths = set()
        self._progress = None
        self._main_task_id = None
        
        # Initialize Firestore client
        self._init_firestore_client(project_id, credentials_path)
        
    def _init_firestore_client(self, project_id: Optional[str], credentials_path: Optional[str]):
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
        return '    ' * level
    
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
            if value_type == list:
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
        
    def describe_fields(self, data: Dict[str, Any], level: int, output_lines: List[str]):
        """Recursively describe fields of a document.
        
        Args:
            data: Document data dictionary
            level: Current nesting level
            output_lines: List to append output lines to
        """
        for key, value in sorted(data.items()):
            field_type = self.describe_type(value)
            output_lines.append(f"{self.indent(level)}- `{key}` ({field_type})")
            self.stats["fields"] += 1
            
            # Recurse into maps/dictionaries
            if isinstance(value, dict):
                self.describe_fields(value, level + 1, output_lines)
    
    def process_collection(
        self, 
        path: str, 
        level: int = 0, 
        parent_task_id: Optional[TaskID] = None
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
                    task_description, 
                    total=self.max_docs,
                    parent=parent_task_id
                )
            
            # Get collection stats if requested
            doc_count = None
            if self.include_stats:
                try:
                    # Efficiently count documents (limited to 1000 for performance)
                    query = collection.limit(1000)
                    doc_count = len(list(query.stream()))
                    doc_count_str = f"{doc_count}+" if doc_count >= 1000 else str(doc_count)
                except Exception as e:
                    logger.warning(f"Failed to count documents in {path}: {e}")
                    doc_count_str = "unknown"
            
            # Add collection header
            collection_header = f"{self.indent(level)}### Collection: `{path}`"
            if doc_count is not None:
                collection_header += f" ({doc_count_str} documents)"
            output_lines.append(collection_header)
            
            # Sample documents
            try:
                docs = list(collection.limit(self.max_docs).stream())
                
                if not docs:
                    output_lines.append(f"{self.indent(level+1)}*No documents found*")
                    return output_lines
                    
                # Process each document
                for i, doc in enumerate(docs):
                    if self._progress and collection_task_id:
                        self._progress.update(collection_task_id, completed=i+1)
                    
                    doc_data = doc.to_dict()
                    if not doc_data:
                        continue
                        
                    self.stats["documents"] += 1
                    output_lines.append(f"{self.indent(level+1)}#### Document: `{doc.id}`")
                    self.describe_fields(doc_data, level + 2, output_lines)
                    
                    # Process subcollections if not at max depth
                    if level < self.max_depth - 1:
                        subcollections = list(doc.reference.collections())
                        for subcol in subcollections:
                            subcol_output = self.process_collection(
                                subcol.path, 
                                level + 2,
                                collection_task_id
                            )
                            output_lines.extend(subcol_output)
                            
            except PermissionDenied:
                output_lines.append(
                    f"{self.indent(level+1)}*Permission denied when accessing documents*"
                )
            except Exception as e:
                output_lines.append(
                    f"{self.indent(level+1)}*Error accessing documents: {str(e)}*"
                )
                logger.error(f"Error processing documents in {path}: {e}")
                
        except Exception as e:
            output_lines.append(f"{self.indent(level)}### `{path}`: *Error: {str(e)}*")
            logger.error(f"Error processing collection {path}: {e}")
            
        return output_lines
    
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
                console=console
            ) as progress:
                self._progress = progress
                self._main_task_id = progress.add_task("Exploring Firestore schema...", total=None)
                
                # Get all top-level collections
                try:
                    collections = list(self.db.collections())
                    if not collections:
                        output.append("*No collections found in the database.*")
                        return "\n".join(output)
                        
                    progress.update(self._main_task_id, total=len(collections))
                    
                    # Process each collection
                    for i, col in enumerate(collections):
                        progress.update(
                            self._main_task_id, 
                            description=f"Processing collection {i+1}/{len(collections)}: {col.id}",
                            completed=i
                        )
                        collection_output = self.process_collection(col.id, parent_task_id=self._main_task_id)
                        output.extend(collection_output)
                        
                    progress.update(self._main_task_id, completed=len(collections))
                    
                except Exception as e:
                    output.append(f"*Error listing collections: {str(e)}*")
                    logger.error(f"Failed to list collections: {e}")
                    
        except Exception as e:
            output.append(f"*Exploration error: {str(e)}*")
            logger.error(f"Exploration error: {e}")
            
        # Add statistics
        if self.include_stats:
            self.stats["duration"] = time.time() - self.stats["start_time"]
            output.append("\n## Statistics")
            output.append(f"- Collections: {self.stats['collections']}")
            output.append(f"- Documents sampled: {self.stats['documents']}")
            output.append(f"- Fields analyzed: {self.stats['fields']}")
            output.append(f"- Duration: {self.stats['duration']:.2f} seconds")
        
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
            
            if format.lower() == 'json':
                # Convert markdown to a structured JSON format
                # This is a simplified implementation
                collections_data = {}
                current_collection = None
                current_document = None
                
                for line in output.split('\n'):
                    if '### Collection: `' in line:
                        collection_name = line.split('`')[1]
                        collections_data[collection_name] = {"documents": {}}
                        current_collection = collection_name
                    elif '#### Document: `' in line and current_collection:
                        doc_id = line.split('`')[1]
                        collections_data[current_collection]["documents"][doc_id] = {"fields": []}
                        current_document = doc_id
                    elif '- `' in line and current_collection and current_document:
                        field_info = line.strip().split('`')[1]
                        field_type = line.split('(')[1].split(')')[0]
                        collections_data[current_collection]["documents"][current_document]["fields"].append({
                            "name": field_info,
                            "type": field_type
                        })
                
                with open(output_path, 'w', encoding='utf-8') as f:
                    json.dump(collections_data, f, indent=2)
            else:
                # Write markdown directly
                with open(output_path, 'w', encoding='utf-8') as f:
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
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    parser.add_argument(
        "--project-id", "-p",
        help="Google Cloud project ID (defaults to environment)"
    )
    
    parser.add_argument(
        "--credentials", "-c",
        help="Path to service account credentials JSON file"
    )
    
    parser.add_argument(
        "--max-docs", "-m",
        type=int, default=5,
        help="Maximum number of documents to sample per collection"
    )
    
    parser.add_argument(
        "--depth", "-d",
        type=int, default=5,
        help="Maximum depth to explore subcollections"
    )
    
    parser.add_argument(
        "--output", "-o",
        default="firestore_schema.md",
        help="Output file path"
    )
    
    parser.add_argument(
        "--format", "-f",
        choices=["md", "json"],
        default="md",
        help="Output format (markdown or JSON)"
    )
    
    parser.add_argument(
        "--no-stats",
        action="store_true",
        help="Don't include statistics in the output"
    )
    
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose logging"
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
            include_stats=not args.no_stats
        )
        
        # Run exploration
        console.print("🔍 Exploring Firestore schema...", style="bold blue")
        schema_doc = explorer.explore_database()
        
        # Export results
        output_path = explorer.export_to_file(schema_doc, args.output, args.format)
        
        # Show preview in terminal
        console.print("\n[bold green]✅ Schema exploration complete![/]")
        console.print(f"Output saved to: [bold]{output_path}[/]")
        
        if args.format == "md":
            console.print("\n[bold]Schema Preview:[/]")
            console.print(Markdown(schema_doc[:2000] + "...\n\n*Preview truncated*" if len(schema_doc) > 2000 else schema_doc))
        
    except Exception as e:
        console.print(f"[bold red]Error:[/] {str(e)}")
        if args.verbose:
            console.print_exception()
        return 1
        
    return 0

if __name__ == "__main__":
    sys.exit(main())
