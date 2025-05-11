# Web Crawler Monitoring System

This system provides a web interface for monitoring a distributed web crawler, adding custom URLs to crawl, and searching indexed content.

## Components

- **Master Node**: Manages the crawling process
- **Crawler Node**: Fetches and processes web pages
- **Indexer Node**: Indexes the content for searching
- **Web App**: Interface for monitoring and controlling the system

## Prerequisites

- Python 3.7+
- Redis (for Celery)
- AWS account with SQS queues and S3 bucket set up

## Environment Setup

Create a `.env` file with the following variables:

```
REDIS_URL=redis://localhost:6379/0
AWS_REGION=your-aws-region
SQS_QUEUE_URL=your-content-queue-url
SQS_URLS_QUEUE=your-urls-queue-url
S3_BUCKET_NAME=your-s3-bucket-name
```

## Installation

1. Install dependencies:

```bash
pip install flask boto3 beautifulsoup4 celery python-dotenv whoosh requests
```

2. Make sure Redis is running for Celery.

## Running the System

Start each component in a separate terminal window:

1. **Master Node**:

```bash
python master_node.py
```

2. **Crawler Node**:

```bash
python crawler_node.py
```

3. **Indexer Node**:

```bash
python indexer_node.py
```

4. **Web App**:

```bash
python webapp.py
```

The web interface will be available at: http://localhost:5000

## Using the Web Interface

1. **Monitor System Status**:
   - View queue sizes and system status on the home page

2. **Add URLs to Crawl**:
   - Enter URLs in the "Add URL to Crawl" section
   - URLs will be added to the queue and processed by crawler nodes
   - **NOTE: The system has no seed URLs - all URLs to crawl must be manually added through this interface**

3. **Search Indexed Content**:
   - Enter search terms in the "Search Indexed Content" section
   - Results will display with links to the original content

## Architecture

- AWS SQS queues are used for communication between components
- Crawled content is indexed using Whoosh
- The index is stored in S3 for persistence
- The web interface communicates directly with the queues to add URLs and retrieve status
- All URLs to crawl must be manually specified - no automatic crawling or seed URLs

## Scaling

To scale the system:

- Run multiple crawler nodes to increase crawling throughput
- Run multiple indexer nodes to increase indexing throughput
- Scale the web app using a WSGI server like Gunicorn for production use
