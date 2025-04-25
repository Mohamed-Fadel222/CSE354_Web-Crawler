# Distributed Web Crawler

A distributed web crawler system implemented using MPI and AWS services.

## Prerequisites

- Python 3.8+
- MPI (Message Passing Interface)
- AWS Account with appropriate permissions
- AWS CLI configured with credentials

## Setup

1. Install the required dependencies:
```bash
pip install -r requirements.txt
```

2. Create an SQS queue in AWS and note its URL.

3. Create a `.env` file in the project root with the following variables:
```
AWS_REGION=us-east-1
SQS_QUEUE_URL=your-sqs-queue-url
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key
```

## Running the System

1. Start the master node:
```bash
mpiexec -n 1 python master_node.py
```

2. Start the crawler nodes:
```bash
mpiexec -n <number_of_crawlers> python craweler_node.py
```

3. Start the indexer node:
```bash
mpiexec -n 1 python indexer_node.py
```

Replace `<number_of_crawlers>` with the desired number of crawler nodes.

## Architecture

The system consists of three main components:

1. **Master Node**: Manages task distribution and worker coordination
2. **Crawler Nodes**: Fetch and parse web pages
3. **Indexer Node**: Indexes content and handles search queries

Communication between components is handled through:
- MPI for direct node-to-node communication
- AWS SQS for asynchronous message passing

## Features

- Distributed web crawling
- Basic politeness measures (crawl delay)
- URL deduplication
- Basic in-memory indexing using Whoosh
- Simple keyword search functionality
- Worker health monitoring
- Error handling and logging

## Testing

To test the system:

1. Start all components as described above
2. The master node will begin distributing seed URLs to crawlers
3. Crawlers will process URLs and send content to the indexer via SQS
4. The indexer will process messages and build the search index

## Monitoring

Logs are available for each component:
- Master node logs
- Crawler node logs
- Indexer node logs

Check the console output for real-time status updates and error messages.
