# Web Crawler System

A distributed web crawler system with persistent storage and search capabilities.

## System Architecture

The system consists of the following components:

1. **Master Node** (`master_node.py`): Manages the crawling process and distributes URLs to be crawled.
2. **Crawler Node** (`crawler_node.py`): Fetches web pages, extracts content and links, and stores data in S3.
3. **Indexer Node** (`indexer_node.py`): Indexes the crawled content using Whoosh for efficient searching.
4. **Search Interface** (`search_interface.py`): Web-based interface for searching through the crawled content.
5. **Standalone Web App** (`standalone_web_app.py`): All-in-one solution that combines crawling and searching in a single interface.

## New! Standalone Web App

A standalone web application has been added that combines all the functionality into a single server:

- **User-driven crawling**: Submit URLs to crawl directly from the web interface
- **Immediate indexing**: Content is indexed as soon as it's crawled
- **S3 Storage**: All content is stored durably in S3
- **Content fixing**: Automatically fix content retrieval errors with one click
- **Tabbed interface**: Easy switching between searching and crawling

### Running the Standalone Web App

To run just the standalone web app without the distributed system:

```
python run_standalone.py
```

The web interface will be available at http://localhost:5000.

## Data Flow

1. The master node initializes seed URLs and sends them to a queue for crawling.
2. Crawler nodes pull URLs from the queue, fetch web pages, extract content, and:
   - Store raw HTML and extracted text in S3 for durability
   - Send extracted URLs back to the queue for further crawling
   - Send extracted text to another queue for indexing
3. The indexer node pulls content from the queue and indexes it using Whoosh.
4. The search interface allows users to search the Whoosh index and retrieves full content from S3.

## Setup

### Prerequisites

- Python 3.8+
- AWS account with SQS and S3 access
- Required Python packages:
  - boto3
  - requests
  - beautifulsoup4
  - whoosh
  - flask
  - python-dotenv

### Configuration

Create a `.env` file in the project root with the following variables:

```
AWS_REGION=eu-north-1
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
SQS_QUEUE_URL=https://sqs.eu-north-1.amazonaws.com/your-account/your-indexer-queue
SQS_URLS_QUEUE=https://sqs.eu-north-1.amazonaws.com/your-account/your-urls-queue
S3_BUCKET_NAME=web-crawler-content
```

### Running the Full Distributed System

1. Start all components at once:
   ```
   python run_system.py
   ```

Or start each component separately:

1. Start the master node:
   ```
   python master_node.py
   ```

2. Start one or more crawler nodes:
   ```
   python crawler_node.py
   ```

3. Start the indexer node:
   ```
   python indexer_node.py
   ```

4. Start the search interface:
   ```
   python search_interface.py
   ```

The search interface will be available at http://localhost:5000.

## Features

- **Distributed Crawling**: Multiple crawler nodes can work in parallel.
- **Data Persistence**: Raw HTML and processed text are stored in S3 for durability.
- **Efficient Indexing**: Whoosh provides fast full-text search capabilities.
- **User-friendly Interface**: Web-based search interface for easy access to crawled content.
- **URL Submission**: Add new URLs to crawl directly from the web interface.
- **Content Error Fixing**: Fix content retrieval errors with one click.

## Troubleshooting

### Error Retrieving Content

If you see "Error retrieving content" in search results, this could be due to:

1. **S3 Access Issues**: Check your AWS credentials and permissions
2. **Missing Content**: The content might not have been properly stored
3. **Bucket Configuration**: S3 bucket might not exist or be misconfigured

Solutions:
- Use the "Re-crawl" button in the web interface to re-fetch and index the content
- Check AWS logs for specific error messages
- Verify S3 bucket settings in the AWS console

## Limitations & Future Work

- Limited to text content; does not process images or other media.
- Basic URL filtering and crawl policies.
- Search interface could be enhanced with more advanced filtering options.
- Could add authentication for admin functions.

---

## 9. Monitoring and Troubleshooting

- **Check SQS Queue:**
  - AWS Console → SQS → Your Queue → Monitoring tab
  - Or use AWS CLI as above
- **Check Logs:**
  - Master, crawler, and indexer logs will show activity and errors
- **Common Issues:**
  - `.env` file missing or incorrect on any node
  - No internet access (check VPC, subnet, route table, public IP)
  - SQS permissions missing (ensure your AWS user can send/receive/delete messages)
  - All dependencies not installed

---

## 10. Stopping the System
- Press `Ctrl+C` in the terminal running the MPI command to stop all nodes.

---

## 11. (Optional) Run on Separate Instances
- You can run each node on a separate EC2 instance:
  - On master: `python3 master_node.py`
  - On crawler: `python3 crawler_node.py`
  - On indexer: `python3 indexer_node.py`
- Make sure all instances have the same `.env` and codebase, and can reach each other if using MPI across hosts.

---

## 12. Useful AWS CLI Commands

- Check SQS queue attributes:
  ```bash
  aws sqs get-queue-attributes --queue-url YOUR_SQS_QUEUE_URL --attribute-names All --region eu-north-1
  ```
- Configure AWS CLI:
  ```bash
  aws configure
  ```

---

## 13. Example .env File
```
AWS_SECRET_ACCESS_KEY=YOUR_SECRET_KEY
AWS_ACCESS_KEY_ID=YOUR_ACCESS_KEY
SQS_QUEUE_URL=https://sqs.eu-north-1.amazonaws.com/961889141183/web-crawler-queue
AWS_REGION=eu-north-1
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
