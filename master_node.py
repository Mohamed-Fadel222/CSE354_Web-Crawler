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
        self.crawled_urls = set()
        self.start_time = datetime.now()
        self.last_status_print = datetime.now()
        self.status_print_interval = timedelta(seconds=10)

    def print_status(self):
        try:
            # Get queue attributes for status
            urls_queue_attrs = sqs_client.get_queue_attributes(
                QueueUrl=SQS_URLS_QUEUE,
                AttributeNames=['ApproximateNumberOfMessages']
            )
            
            content_queue_attrs = sqs_client.get_queue_attributes(
                QueueUrl=SQS_QUEUE_URL,
                AttributeNames=['ApproximateNumberOfMessages']
            )
            
            urls_queue_size = int(urls_queue_attrs['Attributes']['ApproximateNumberOfMessages'])
            content_queue_size = int(content_queue_attrs['Attributes']['ApproximateNumberOfMessages'])
            
            logger.info("\n=== System Status ===")
            logger.info(f"URLs Queue Size: {urls_queue_size}")
            logger.info(f"Content Queue Size: {content_queue_size}")
            logger.info(f"Total URLs Crawled: {len(self.crawled_urls)}")
            logger.info(f"System Uptime: {datetime.now() - self.start_time}")
            logger.info("==================\n")
        except Exception as e:
            logger.error(f"Error getting queue status: {str(e)}")

def master_process():
    logger.info("Master node started")
    logger.info("No seed URLs will be added - waiting for URLs from web interface")
    master = MasterNode()
    
    while True:
        try:
            current_time = datetime.now()
            
            # Print status periodically
            if current_time - master.last_status_print > master.status_print_interval:
                master.print_status()
                master.last_status_print = current_time
            
            time.sleep(1)  # Small delay to prevent CPU overuse
            
        except Exception as e:
            logger.error(f"Error in master process: {str(e)}")
            time.sleep(5)  # Wait before retrying

if __name__ == '__main__':
    master_process() 