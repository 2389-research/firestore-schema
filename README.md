# Firestore Schema Dump

A robust tool for exploring and documenting Firestore database schemas.

When we are using codegen tools to interface with firestore, it is often nice to have a dump of the schema to pass into the context.

## Features

- Auto-detects schema from existing documents
- Rich markdown output with type information
- Support for all Firestore data types
- Customizable exploration depth and document sampling
- Error handling and logging
- Export options (markdown, JSON)
- Progress tracking for large databases
- Timeout handling to prevent hanging

## Installation

### Easy

```bash
uv tool install git+https://github.com/2389-research/firestore-schema.git
```

## Usage

After installation, you can use the tool in two ways:

### As a command-line utility

```bash
# Basic usage
firestore-schema --project-id your-project-id

# With credentials file
firestore-schema --credentials /path/to/credentials.json

# Full options
firestore-schema \
    --project-id your-project-id \
    --credentials /path/to/credentials.json \
    --max-docs 10 \
    --depth 3 \
    --timeout 60 \
    --output schema.md \
    --format md \
    --verbose
```

### Or directly using the Python script

```bash
git clone https://github.com/2389-research/firestore-schema.git
cd firestore-schema
```

```bash
# Basic usage
uv run main.py --project-id your-project-id

# With credentials file
uv run main.py --credentials /path/to/credentials.json

# Full options
uv run main.py \
    --project-id your-project-id \
    --credentials /path/to/credentials.json \
    --max-docs 10 \
    --depth 3 \
    --timeout 60 \
    --output schema.md \
    --format md \
    --verbose
```

### Command-line options

- `--project-id`, `-p`: Google Cloud project ID (defaults to environment)
- `--credentials`, `-c`: Path to service account credentials JSON file
- `--max-docs`, `-m`: Maximum number of documents to sample per collection (default: 5)
- `--depth`, `-d`: Maximum depth to explore subcollections (default: 5)
- `--output`, `-o`: Output file path (default: <project_id>.schema.md)
- `--format`, `-f`: Output format (md or json, default: md)
- `--no-stats`: Don't include statistics in the output
- `--timeout`, `-t`: Timeout in seconds for Firestore operations (default: 30)
- `--verbose`, `-v`: Enable verbose logging

## Example Output

```markdown
# Firestore Schema Explorer

Generated on 2023-03-01 12:34:56
Project: `your-project-id`

### Collection: `users` (150 documents)

#### Document: `user123`

- `name` (string)
- `email` (string)
- `age` (integer)
- `isActive` (boolean)
- `metadata` (map)
    - `lastLogin` (timestamp)
    - `preferences` (map)
        - `theme` (string)
- `tags` (array<string>)

### Collection: `products` (75 documents)

#### Document: `product456`

- `name` (string)
- `price` (float)
- `categories` (array<string>)
- `inStock` (boolean)
- `createdAt` (timestamp)

## Statistics

- Collections: 10
- Documents sampled: 35
- Fields analyzed: 124
- Duration: 3.25 seconds
- Timeouts: 0
- Errors: 0
```

## Development

### Running Tests

```bash

# Run all tests
uv run pytest

# Run specific test module
uv run pytest tests/unit/test_timeout.py

# Run with coverage
uv run pytest --cov=main tests/
```

### Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request
