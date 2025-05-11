import time
import logging
import boto3
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
import json
import random
import requests
from botocore.exceptions import ClientError

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('Master')

# AWS Configuration
AWS_REGION = os.getenv('AWS_REGION', 'eu-north-1')
SQS_QUEUE_URL = os.getenv('SQS_QUEUE_URL')  # Content queue
SQS_URLS_QUEUE = os.getenv('SQS_URLS_QUEUE')  # URLs queue
SQS_DLQ_URL = os.getenv('SQS_DLQ_URL', '')  # Dead Letter Queue
HEALTH_CHECK_INTERVAL = int(os.getenv('HEALTH_CHECK_INTERVAL', '300'))  # 5 minutes
CRAWLER_NODES = os.getenv('CRAWLER_NODES', '').split(',')  # List of crawler nodes
INDEXER_NODES = os.getenv('INDEXER_NODES', '').split(',')  # List of indexer nodes

logger.info(f"Using SQS Queue URL: {SQS_QUEUE_URL}")
logger.info(f"Using AWS Region: {AWS_REGION}")
logger.info(f"Monitoring crawler nodes: {CRAWLER_NODES}")
logger.info(f"Monitoring indexer nodes: {INDEXER_NODES}")

sqs_client = boto3.client('sqs', region_name=AWS_REGION)

class MasterNode:
    def __init__(self):
        self.urls_to_crawl = set()  # Using set to avoid duplicates
        self.crawled_urls = set()
        self.start_time = datetime.now()
        self.last_status_print = datetime.now()
        self.last_health_check = time.time()
        self.status_print_interval = timedelta(seconds=30)
        self.health_check_interval = timedelta(seconds=HEALTH_CHECK_INTERVAL)
        self.node_statuses = {
            'crawler': {'status': 'unknown', 'last_check': None},
            'indexer': {'status': 'unknown', 'last_check': None}
        }
        self.system_health = 'initializing'
        self.stats = {
            'urls_sent': 0,
            'urls_failed': 0,
            'health_checks': 0,
            'start_time': datetime.now().isoformat()
        }
        
        # Message type tags
        self.message_types = {
            'URL_TO_CRAWL': 0,           # URL to be crawled
            'CONTENT_TO_INDEX': 1,        # Content to be indexed
            'HEALTH_CHECK': 99,          # Health check
            'SYSTEM_ERROR': 999          # Error notification
        }

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
        """Print system status to logs"""
        logger.info("\n=== System Status ===")
        logger.info(f"URLs Queue Size: {len(self.urls_to_crawl)}")
        logger.info(f"Total URLs Crawled: {len(self.crawled_urls)}")
        logger.info(f"System Uptime: {datetime.now() - self.start_time}")
        logger.info(f"System Health: {self.system_health}")
        logger.info(f"Crawler Nodes: {self.node_statuses['crawler']['status']}")
        logger.info(f"Indexer Nodes: {self.node_statuses['indexer']['status']}")
        logger.info("==================\n")

    def get_queue_attributes(self, queue_url):
        """Get attributes for a queue"""
        try:
            response = sqs_client.get_queue_attributes(
                QueueUrl=queue_url,
                AttributeNames=[
                    'ApproximateNumberOfMessages',
                    'ApproximateNumberOfMessagesNotVisible',
                    'ApproximateNumberOfMessagesDelayed'
                ]
            )
            return response['Attributes']
        except Exception as e:
            logger.error(f"Error getting queue attributes for {queue_url}: {str(e)}")
            return {
                'ApproximateNumberOfMessages': -1,
                'ApproximateNumberOfMessagesNotVisible': -1,
                'ApproximateNumberOfMessagesDelayed': -1
            }

    def check_node_health(self, node_url):
        """Check health of a node by calling its health endpoint"""
        try:
            response = requests.get(f"{node_url}/health", timeout=5)
            if response.status_code == 200:
                return True, response.json()
            return False, {"error": f"Health check failed with status {response.status_code}"}
        except Exception as e:
            return False, {"error": str(e)}

    def perform_health_check(self):
        """Perform a health check on all system components"""
        logger.info("Performing system health check...")
        self.last_health_check = time.time()
        self.stats['health_checks'] += 1
        
        health_issues = []
        
        # Check URL queue
        try:
            url_queue_attrs = self.get_queue_attributes(SQS_URLS_QUEUE)
            url_queue_size = int(url_queue_attrs['ApproximateNumberOfMessages'])
            logger.info(f"URL queue has {url_queue_size} messages")
        except Exception as e:
            health_issues.append(f"URL queue error: {str(e)}")
        
        # Check content queue
        try:
            content_queue_attrs = self.get_queue_attributes(SQS_QUEUE_URL)
            content_queue_size = int(content_queue_attrs['ApproximateNumberOfMessages'])
            logger.info(f"Content queue has {content_queue_size} messages")
        except Exception as e:
            health_issues.append(f"Content queue error: {str(e)}")
            
        # Check crawler nodes
        crawler_healthy = False
        for node in CRAWLER_NODES:
            if node:
                success, data = self.check_node_health(node)
                if success:
                    crawler_healthy = True
                    break
                    
        self.node_statuses['crawler'] = {
            'status': 'healthy' if crawler_healthy else 'unhealthy',
            'last_check': datetime.now().isoformat()
        }
        
        if not crawler_healthy and CRAWLER_NODES:
            health_issues.append("All crawler nodes are unhealthy")
            
        # Check indexer nodes
        indexer_healthy = False
        for node in INDEXER_NODES:
            if node:
                success, data = self.check_node_health(node)
                if success:
                    indexer_healthy = True
                    break
                    
        self.node_statuses['indexer'] = {
            'status': 'healthy' if indexer_healthy else 'unhealthy',
            'last_check': datetime.now().isoformat()
        }
        
        if not indexer_healthy and INDEXER_NODES:
            health_issues.append("All indexer nodes are unhealthy")
        
        # Update system health status
        if not health_issues:
            self.system_health = 'healthy'
            logger.info("Health check completed: System is healthy")
        else:
            self.system_health = 'degraded'
            logger.warning(f"Health check completed: System is degraded - {'; '.join(health_issues)}")
            
        return (self.system_health == 'healthy')

    def get_status(self):
        """Get current system status as JSON"""
        url_queue_attrs = self.get_queue_attributes(SQS_URLS_QUEUE)
        content_queue_attrs = self.get_queue_attributes(SQS_QUEUE_URL)
        
        uptime_seconds = (datetime.now() - datetime.fromisoformat(self.stats['start_time'])).total_seconds()
        urls_per_minute = 0
        if uptime_seconds > 0:
            urls_per_minute = (self.stats['urls_sent'] / (uptime_seconds / 60))
            
        return {
            "system_health": self.system_health,
            "uptime_seconds": int(uptime_seconds),
            "uptime_formatted": str(datetime.now() - datetime.fromisoformat(self.stats['start_time'])).split('.')[0],
            "urls_to_crawl": len(self.urls_to_crawl),
            "urls_crawled": len(self.crawled_urls),
            "urls_sent": self.stats['urls_sent'],
            "urls_failed": self.stats['urls_failed'],
            "urls_rate": round(urls_per_minute, 2),
            "url_queue_size": int(url_queue_attrs['ApproximateNumberOfMessages']),
            "url_queue_in_flight": int(url_queue_attrs['ApproximateNumberOfMessagesNotVisible']),
            "content_queue_size": int(content_queue_attrs['ApproximateNumberOfMessages']),
            "content_queue_in_flight": int(content_queue_attrs['ApproximateNumberOfMessagesNotVisible']),
            "node_statuses": self.node_statuses,
            "last_health_check": time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(self.last_health_check))
        }

    def send_urls_to_queue(self):
        """Send URLs to the queue with message type tags"""
        urls_sent = 0
        urls_failed = 0
        
        # Send URLs to the queue
        for url in list(self.urls_to_crawl)[:10]:  # Send up to 10 URLs at a time
            try:
                sqs_client.send_message(
                    QueueUrl=SQS_URLS_QUEUE,
                    MessageBody=json.dumps({
                        'url': url,
                        'message_type': 'URL_TO_CRAWL',
                        'message_type_tag': self.message_types['URL_TO_CRAWL'],
                        'timestamp': datetime.now().isoformat(),
                        'source': 'master_node'
                    })
                )
                self.crawled_urls.add(url)
                self.urls_to_crawl.remove(url)
                urls_sent += 1
                logger.info(f"Sent URL to queue: {url}")
            except Exception as e:
                urls_failed += 1
                logger.error(f"Error sending URL to queue: {str(e)}")
        
        self.stats['urls_sent'] += urls_sent
        self.stats['urls_failed'] += urls_failed
        
        return urls_sent

def master_process():
    logger.info("Master node started")
    master = MasterNode()
    master.initialize_seed_urls()
    
    # Initial health check
    master.perform_health_check()
    
    while True:
        try:
            current_time = datetime.now()
            
            # Print status periodically
            if current_time - master.last_status_print > master.status_print_interval:
                master.print_status()
                master.last_status_print = current_time
            
            # Perform health check periodically
            if current_time - datetime.fromtimestamp(master.last_health_check) > master.health_check_interval:
                master.perform_health_check()
            
            # Send URLs to queue
            urls_sent = master.send_urls_to_queue()
            
            # Adaptive sleep - sleep longer if no URLs were sent
            if urls_sent == 0:
                # Add some jitter to prevent thundering herd problem
                time.sleep(random.uniform(2, 5))
            else:
                time.sleep(1)  # Small delay to prevent CPU overuse
            
        except Exception as e:
            logger.error(f"Error in master process: {str(e)}")
            time.sleep(5)  # Wait before retrying

def status_api():
    """This function would be called by an API endpoint to get system status"""
    master = MasterNode()
    return master.get_status()

if __name__ == '__main__':
    master_process() 