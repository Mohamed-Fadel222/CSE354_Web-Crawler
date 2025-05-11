import time
import logging
import boto3
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
import json

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('Master')

# AWS Configuration
AWS_REGION = os.getenv('AWS_REGION', 'eu-north-1')
SQS_QUEUE_URL = os.getenv('SQS_QUEUE_URL')
SQS_URLS_QUEUE = os.getenv('SQS_URLS_QUEUE')
logger.info(f"Using SQS Queue URL: {SQS_QUEUE_URL}")
logger.info(f"Using AWS Region: {AWS_REGION}")
sqs_client = boto3.client('sqs', region_name=AWS_REGION)

class MasterNode:
    def __init__(self):
        self.urls_to_crawl = set()  # Using set to avoid duplicates
        self.crawled_urls = set()
        self.start_time = datetime.now()
        self.last_status_print = datetime.now()
        self.status_print_interval = timedelta(seconds=10)

    def initialize_seed_urls(self):
        # Start with simple, reliable websites
        seed_urls = [
            "http://example.com",
            "http://httpbin.org",
            "http://httpstat.us",
            "http://example.org",
            "http://example.net"
        ]
        self.urls_to_crawl.update(seed_urls)
        logger.info(f"Initialized with seed URLs: {seed_urls}")

    def print_status(self):
        logger.info("\n=== System Status ===")
        logger.info(f"URLs Queue Size: {len(self.urls_to_crawl)}")
        logger.info(f"Total URLs Crawled: {len(self.crawled_urls)}")
        logger.info(f"System Uptime: {datetime.now() - self.start_time}")
        logger.info("==================\n")

    def send_urls_to_queue(self):
        # Send URLs to the queue
        for url in list(self.urls_to_crawl)[:10]:  # Send up to 10 URLs at a time
            try:
                sqs_client.send_message(
                    QueueUrl=SQS_URLS_QUEUE,
                    MessageBody=json.dumps({'url': url})
                )
                self.crawled_urls.add(url)
                self.urls_to_crawl.remove(url)
                logger.info(f"Sent URL to queue: {url}")
            except Exception as e:
                logger.error(f"Error sending URL to queue: {str(e)}")

def master_process():
    logger.info("Master node started")
    master = MasterNode()
    master.initialize_seed_urls()
    
    while True:
        try:
            current_time = datetime.now()
            
            # Print status periodically
            if current_time - master.last_status_print > master.status_print_interval:
                master.print_status()
                master.last_status_print = current_time
            
            # Send URLs to queue
            master.send_urls_to_queue()
            
            time.sleep(1)  # Small delay to prevent CPU overuse
            
        except Exception as e:
            logger.error(f"Error in master process: {str(e)}")
            time.sleep(5)  # Wait before retrying

if __name__ == '__main__':
    master_process() 