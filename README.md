# Distributed Web Crawler

A distributed web crawler system implemented using MPI and AWS services.

## Features

- **Distributed Web Crawling**: Multi-node architecture with master, crawler, and indexer nodes
- **Task Queue**: AWS SQS for distributed task management
- **Content Indexing**: Whoosh for full-text search capabilities
- **Error Handling**: Comprehensive fault tolerance with message metadata and retries
- **Health Monitoring**: System-wide health checks and monitoring
- **Scalability**: Horizontally scalable architecture
- **Cloud-Ready**: Designed for cloud deployment with AWS services

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

2. Create AWS SQS queues:
   - Create a queue for crawl URLs (e.g., web-crawler-urls-queue)
   - Create a queue for content indexing (e.g., web-crawler-content-queue)
   - Configure appropriate visibility timeout (at least 30 seconds)

3. Create an S3 bucket for index storage

4. Create a `.env` file in the project root with the following variables:
```
AWS_REGION=eu-north-1
SQS_QUEUE_URL=https://sqs.eu-north-1.amazonaws.com/YOUR-ACCOUNT-ID/web-crawler-content-queue
SQS_URLS_QUEUE=https://sqs.eu-north-1.amazonaws.com/YOUR-ACCOUNT-ID/web-crawler-urls-queue
S3_BUCKET_NAME=your-s3-bucket-name
AWS_ACCESS_KEY_ID=YOUR_ACCESS_KEY
AWS_SECRET_ACCESS_KEY=YOUR_SECRET_KEY
REDIS_URL=redis://localhost:6379/0
MAX_RETRIES=3
HEALTH_CHECK_INTERVAL=300
CRAWLER_NODES=http://crawler-node-1:5002,http://crawler-node-2:5002
INDEXER_NODES=http://indexer-node-1:5003,http://indexer-node-2:5003
MASTER_NODE_URL=http://master-node:5001
```

## Running the System

### Option 1: MPI (Single Machine)

```bash
mpiexec --oversubscribe -n 3 python3 -m mpi4py master_node.py
```
This will start the master, crawler, and indexer nodes as separate MPI processes.

### Option 2: Separate Processes (Development)

```bash
# Terminal 1: Start master node
python master_node.py

# Terminal 2: Start crawler node
python crawler_node.py

# Terminal 3: Start indexer node
python indexer_node.py

# Terminal 4: Start webapp
python webapp.py
```

### Option 3: Cloud Deployment

For cloud deployment, run each component on a separate VM:
- Master node: `python master_node.py` (with proper .env configuration)
- Crawler nodes: `python crawler_node.py` (can run multiple instances)
- Indexer nodes: `python indexer_node.py` (can run multiple instances)
- Web app: `python webapp.py` (user interface)

## Testing the System

1. **Set up environment variables**
   Make sure your .env file is properly configured with AWS credentials and queue URLs.

2. **Start the system components**
   Using one of the options described above.

3. **Monitor the logs**
   Each component will output logs showing its operation.

4. **Test the crawler**
   - Access the web interface at http://localhost:5000
   - Enter URLs to crawl in the form and submit
   - Monitor the crawler logs to see processing

5. **Test the search**
   - After content has been crawled and indexed
   - Visit http://localhost:5000/search
   - Enter search queries to retrieve results

6. **Test fault tolerance**
   - Forcibly stop one of the components (e.g., crawler_node.py)
   - Observe messages being requeued with delay
   - Restart the component and observe processing resume
   - Check logs for retry behavior

7. **Monitor system status**
   - Access http://localhost:5000/monitor
   - View health status, queue depths, and node statuses
   - Watch for any health warnings in the logs

## Fault Tolerance Architecture

The system implements fault tolerance through several mechanisms:

1. **Message Retry**
   - Failed messages are marked with error_count and requeued
   - Exponential backoff with increasing delay (60s * retry_count)
   - Maximum retry count configurable via MAX_RETRIES

2. **Health Monitoring**
   - All nodes perform periodic health checks
   - Queue health monitoring detects stuck processing
   - Node health is monitored via HTTP endpoints

3. **Message Persistence**
   - SQS provides durable storage of tasks
   - S3 stores the search index for durability
   - Visibility timeout prevents duplicate processing

4. **Error Tracking**
   - Failed messages contain detailed error information
   - Error counts and timestamps are tracked
   - System logs provide diagnostic information

## Architecture

The system consists of three main components:

1. **Master Node**: Manages task distribution and worker coordination
2. **Crawler Nodes**: Fetch and parse web pages
3. **Indexer Node**: Indexes content and handles search queries

Communication between components is handled through:
- MPI for direct node-to-node communication
- AWS SQS for asynchronous message passing

## Monitoring

### Check SQS Queue Status
```bash
aws sqs get-queue-attributes --queue-url YOUR_SQS_QUEUE_URL --attribute-names All --region eu-north-1
```

### View Component Logs
Each component will output detailed logs including:
- Processing activity
- Error handling
- Health status
- Performance metrics

### Health Endpoints
- Master: http://master-node:5001/health
- Web app: http://webapp:5000/health

## Stopping the System
- Press `Ctrl+C` in the terminal running each process
- For MPI, stopping the master process will stop all nodes
