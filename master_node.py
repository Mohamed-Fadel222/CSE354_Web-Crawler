import time
import logging
import boto3
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
import json
import socket
import sys
import traceback
from botocore.exceptions import ClientError

# Message type tag constants for consistent tagging
class MessageTags:
    # URL message types
    URL_NEW = 0         # Fresh URL to be crawled
    URL_RETRY = 1       # URL being retried after a failure
    URL_PRIORITY = 2    # High priority URL to crawl next
    
    # Content message types
    CONTENT_TEXT = 10   # Regular text content
    CONTENT_HTML = 11   # Raw HTML content
    CONTENT_MEDIA = 12  # Media content or reference
    
    # Processing status
    STATUS_SUCCESS = 20   # Processing completed successfully
    STATUS_WARNING = 21   # Processing had warnings
    STATUS_ERROR = 22     # Processing had errors
    
    # System messages
    SYSTEM_INFO = 90      # System information message
    SYSTEM_CONFIG = 91    # Configuration update
    SYSTEM_COMMAND = 92   # Command to crawler nodes
    
    # Special messages
    HEARTBEAT = 99        # Heartbeat/keep-alive message
    SHUTDOWN = 999        # System shutdown signal

# Load environment variables
load_dotenv()

# Configure logging
os.makedirs('logs', exist_ok=True)
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/master_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger('Master')

# Log message tag definitions
logger.info("Message tag definitions:")
for tag_name in dir(MessageTags):
    if not tag_name.startswith('__'):
        tag_value = getattr(MessageTags, tag_name)
        logger.info(f"  {tag_name} = {tag_value}")

# AWS Configuration
AWS_REGION = os.getenv('AWS_REGION', 'eu-north-1')
SQS_QUEUE_URL = os.getenv('SQS_QUEUE_URL')
SQS_URLS_QUEUE = os.getenv('SQS_URLS_QUEUE')
SQS_DLQ_URL = os.getenv('SQS_DLQ_URL')  # Dead Letter Queue for failed messages

logger.info(f"Using SQS Queue URL: {SQS_QUEUE_URL}")
logger.info(f"Using URLs Queue URL: {SQS_URLS_QUEUE}")
logger.info(f"Using AWS Region: {AWS_REGION}")
logger.info(f"Dead Letter Queue: {SQS_DLQ_URL if SQS_DLQ_URL else 'Not configured'}")

# Initialize AWS clients with retry configuration
try:
    session = boto3.Session(region_name=AWS_REGION)
    sqs_client = session.client('sqs', 
                            config=boto3.session.Config(
                                retries={'max_attempts': 5, 'mode': 'standard'},
                                connect_timeout=5,
                                read_timeout=10
                            ))
    logger.info("AWS clients initialized with retry configuration")
except Exception as e:
    logger.critical(f"Failed to initialize AWS clients: {str(e)}")
    logger.critical(traceback.format_exc())
    sys.exit(1)

class MasterNode:
    def __init__(self):
        self.crawled_urls = set()
        self.start_time = datetime.now()
        self.last_status_print = datetime.now()
        self.status_print_interval = timedelta(seconds=10)
        self.last_heartbeat_time = datetime.now()
        self.heartbeat_interval = timedelta(seconds=60)
        self.hostname = socket.gethostname()
        self.master_id = f"master-{self.hostname[:8]}"
        logger.info(f"Master node initialized with ID: {self.master_id}")

    def print_status(self):
        try:
            # Get queue attributes for status
            urls_queue_attrs = sqs_client.get_queue_attributes(
                QueueUrl=SQS_URLS_QUEUE,
                AttributeNames=['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible']
            )
            
            content_queue_attrs = sqs_client.get_queue_attributes(
                QueueUrl=SQS_QUEUE_URL,
                AttributeNames=['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible']
            )
            
            # Get both visible and in-flight messages
            urls_queue_visible = int(urls_queue_attrs['Attributes']['ApproximateNumberOfMessages'])
            urls_queue_inflight = int(urls_queue_attrs['Attributes']['ApproximateNumberOfMessagesNotVisible'])
            urls_queue_size = urls_queue_visible + urls_queue_inflight
            
            content_queue_visible = int(content_queue_attrs['Attributes']['ApproximateNumberOfMessages'])
            content_queue_inflight = int(content_queue_attrs['Attributes']['ApproximateNumberOfMessagesNotVisible'])
            content_queue_size = content_queue_visible + content_queue_inflight
            
            logger.info("\n=== System Status ===")
            logger.info(f"URLs Queue Size: {urls_queue_size} ({urls_queue_visible} visible, {urls_queue_inflight} in flight)")
            logger.info(f"Content Queue Size: {content_queue_size} ({content_queue_visible} visible, {content_queue_inflight} in flight)")
            logger.info(f"Total URLs Crawled: {len(self.crawled_urls)}")
            logger.info(f"System Uptime: {datetime.now() - self.start_time}")
            logger.info("==================\n")
            
            # Send status as a system info message
            try:
                status_message = {
                    'master_id': self.master_id,
                    'hostname': self.hostname,
                    'timestamp': datetime.now().isoformat(),
                    'tag': MessageTags.SYSTEM_INFO,
                    'status': {
                        'urls_queue_size': urls_queue_size,
                        'urls_queue_visible': urls_queue_visible,
                        'urls_queue_inflight': urls_queue_inflight,
                        'content_queue_size': content_queue_size,
                        'content_queue_visible': content_queue_visible,
                        'content_queue_inflight': content_queue_inflight,
                        'crawled_urls_count': len(self.crawled_urls),
                        'uptime_seconds': (datetime.now() - self.start_time).total_seconds()
                    }
                }
                sqs_client.send_message(
                    QueueUrl=SQS_URLS_QUEUE,
                    MessageBody=json.dumps(status_message)
                )
                logger.info(f"Sent system status with tag {MessageTags.SYSTEM_INFO}")
            except Exception as e:
                logger.warning(f"Could not send system status: {str(e)}")
                
        except Exception as e:
            logger.error(f"Error getting queue status: {str(e)}")
            
    def send_heartbeat(self):
        """Send a heartbeat message to show the master node is active"""
        try:
            heartbeat = {
                'master_id': self.master_id,
                'hostname': self.hostname,
                'timestamp': datetime.now().isoformat(),
                'tag': MessageTags.HEARTBEAT,
                'status': 'running',
                'uptime_seconds': (datetime.now() - self.start_time).total_seconds()
            }
            sqs_client.send_message(
                QueueUrl=SQS_URLS_QUEUE,
                MessageBody=json.dumps(heartbeat)
            )
            logger.info(f"Sent heartbeat with tag {MessageTags.HEARTBEAT}")
        except Exception as e:
            logger.warning(f"Could not send heartbeat: {str(e)}")

def master_process():
    logger.info("Master node started")
    logger.info("No seed URLs will be added - waiting for URLs from web interface")
    master = MasterNode()
    
    # Send startup heartbeat
    master.send_heartbeat()
    
    while True:
        try:
            current_time = datetime.now()
            
            # Print status periodically
            if current_time - master.last_status_print > master.status_print_interval:
                master.print_status()
                master.last_status_print = current_time
                
            # Send heartbeat periodically
            if current_time - master.last_heartbeat_time > master.heartbeat_interval:
                master.send_heartbeat()
                master.last_heartbeat_time = current_time
            
            time.sleep(1)  # Small delay to prevent CPU overuse
            
        except Exception as e:
            logger.error(f"Error in master process: {str(e)}")
            logger.error(traceback.format_exc())
            time.sleep(5)  # Wait before retrying

if __name__ == '__main__':
    # Check if we're sending a command
    if len(sys.argv) > 1 and sys.argv[1] == 'command':
        if len(sys.argv) < 3:
            print("Usage: python master_node.py command [shutdown|status]")
            sys.exit(1)
            
        command = sys.argv[2]
        
        try:
            # Initialize SQS client
            session = boto3.Session(region_name=AWS_REGION)
            sqs_client = session.client('sqs')
            
            if command == 'shutdown':
                # Send shutdown command to all nodes
                message = {
                    'tag': MessageTags.SHUTDOWN,
                    'timestamp': datetime.now().isoformat(),
                    'command': 'shutdown',
                    'sender': socket.gethostname()
                }
                response = sqs_client.send_message(
                    QueueUrl=SQS_URLS_QUEUE,
                    MessageBody=json.dumps(message)
                )
                print(f"Shutdown command sent. MessageId: {response.get('MessageId')}")
                
            elif command == 'status':
                # Send a command to get status from all nodes
                message = {
                    'tag': MessageTags.SYSTEM_COMMAND,
                    'command': 'report-status',
                    'timestamp': datetime.now().isoformat(),
                    'sender': socket.gethostname()
                }
                response = sqs_client.send_message(
                    QueueUrl=SQS_URLS_QUEUE,
                    MessageBody=json.dumps(message)
                )
                print(f"Status command sent. MessageId: {response.get('MessageId')}")
                
            else:
                print(f"Unknown command: {command}")
                print("Available commands: shutdown, status")
                sys.exit(1)
                
            sys.exit(0)
            
        except Exception as e:
            print(f"Error sending command: {str(e)}")
            traceback.print_exc()
            sys.exit(1)
            
    master_process() 