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

# Message Tags
MSG_TAG_INFO = 0       # Regular informational messages
MSG_TAG_URL = 1        # URL processing messages
MSG_TAG_CONTENT = 2    # Content processing messages  
MSG_TAG_WARNING = 99   # Warning messages
MSG_TAG_ERROR = 999    # Error messages

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
            
            # Send status message to content queue
            status_message = {
                'tag': MSG_TAG_INFO,
                'message_type': 'system_status',
                'timestamp': datetime.now().isoformat(),
                'urls_queue_size': urls_queue_size,
                'content_queue_size': content_queue_size,
                'urls_crawled': len(self.crawled_urls),
                'uptime_seconds': (datetime.now() - self.start_time).total_seconds()
            }
            
            # Send the status message to the queue for webapp to consume
            sqs_client.send_message(
                QueueUrl=SQS_QUEUE_URL,
                MessageBody=json.dumps(status_message)
            )
            
            logger.info("\n=== System Status ===")
            logger.info(f"URLs Queue Size: {urls_queue_size}")
            logger.info(f"Content Queue Size: {content_queue_size}")
            logger.info(f"Total URLs Crawled: {len(self.crawled_urls)}")
            logger.info(f"System Uptime: {datetime.now() - self.start_time}")
            logger.info("==================\n")
        except Exception as e:
            logger.error(f"Error getting queue status: {str(e)}")
            
    def add_url_to_queue(self, url, source="master_node"):
        """Add a URL to the crawler queue with proper tagging"""
        try:
            sqs_client.send_message(
                QueueUrl=SQS_URLS_QUEUE,
                MessageBody=json.dumps({
                    'tag': MSG_TAG_URL,
                    'url': url,
                    'source': source,
                    'timestamp': datetime.now().isoformat()
                })
            )
            logger.info(f"Added URL to queue: {url}")
            return True
        except Exception as e:
            logger.error(f"Error adding URL to queue: {str(e)}")
            
            # Send error message
            try:
                sqs_client.send_message(
                    QueueUrl=SQS_QUEUE_URL,
                    MessageBody=json.dumps({
                        'tag': MSG_TAG_ERROR,
                        'error': f"Failed to add URL to queue: {str(e)}",
                        'url': url,
                        'timestamp': datetime.now().isoformat()
                    })
                )
            except:
                pass
                
            return False

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
            # Send error message to queue
            try:
                sqs_client.send_message(
                    QueueUrl=SQS_QUEUE_URL,
                    MessageBody=json.dumps({
                        'tag': MSG_TAG_ERROR,
                        'error': f"Master node error: {str(e)}",
                        'timestamp': datetime.now().isoformat()
                    })
                )
            except Exception as send_err:
                logger.error(f"Error sending error message: {str(send_err)}")
                
            time.sleep(5)  # Wait before retrying

if __name__ == '__main__':
    master_process() 