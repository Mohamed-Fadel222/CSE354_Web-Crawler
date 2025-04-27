from mpi4py import MPI
import time
import logging
import boto3
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
from collections import deque

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('Master')

# AWS Configuration
AWS_REGION = os.getenv('AWS_REGION', 'eu-north-1')
SQS_QUEUE_URL = os.getenv('SQS_QUEUE_URL')
logger.info(f"Using SQS Queue URL: {SQS_QUEUE_URL}")
logger.info(f"Using AWS Region: {AWS_REGION}")
sqs_client = boto3.client('sqs', region_name=AWS_REGION)

class MasterNode:
    def __init__(self, comm, rank, size):
        self.comm = comm
        self.rank = rank
        self.size = size
        self.crawler_nodes = size - 2  # Assuming master and at least one indexer node
        self.indexer_nodes = 1
        self.active_crawler_nodes = list(range(1, 1 + self.crawler_nodes))
        self.active_indexer_nodes = list(range(1 + self.crawler_nodes, size))
        self.urls_to_crawl = set()  # Using set to avoid duplicates
        self.crawled_urls = set()
        self.crawler_status = {node: {'last_heartbeat': datetime.now(), 'active': True, 'urls_processed': 0} 
                             for node in self.active_crawler_nodes}
        self.heartbeat_timeout = timedelta(seconds=60)  # Increased timeout
        self.last_status_print = datetime.now()
        self.status_print_interval = timedelta(seconds=10)
        self.start_time = datetime.now()  # Track system start time
        self.url_batch_size = 3  # Number of URLs to assign per crawler

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

    def check_crawler_health(self):
        current_time = datetime.now()
        
        # Print status periodically
        if current_time - self.last_status_print > self.status_print_interval:
            self.print_status()
            self.last_status_print = current_time
        
        for node, status in self.crawler_status.items():
            if current_time - status['last_heartbeat'] > self.heartbeat_timeout:
                if status['active']:
                    logger.warning(f"Crawler node {node} has not sent heartbeat in {self.heartbeat_timeout}")
                    status['active'] = False

    def print_status(self):
        logger.info("\n=== System Status ===")
        logger.info(f"URLs Queue Size: {len(self.urls_to_crawl)}")
        logger.info(f"Total URLs Crawled: {len(self.crawled_urls)}")
        logger.info("\nCrawler Status:")
        for node, status in self.crawler_status.items():
            state = "🟢 ACTIVE" if status['active'] else "🔴 INACTIVE"
            urls = status['urls_processed']
            last_beat = datetime.now() - status['last_heartbeat']
            logger.info(f"Crawler {node}: {state}")
            logger.info(f"  ├─ URLs Processed: {urls}")
            logger.info(f"  └─ Last Heartbeat: {last_beat.seconds}s ago")
        logger.info("\nSystem Health:")
        active_crawlers = sum(1 for s in self.crawler_status.values() if s['active'])
        logger.info(f"Active Crawlers: {active_crawlers}/{len(self.crawler_status)}")
        logger.info(f"Processing Rate: {sum(s['urls_processed'] for s in self.crawler_status.values())/max(1, (datetime.now() - self.start_time).total_seconds()/60):.2f} URLs/minute")
        logger.info("==================\n")

    def process_crawler_message(self, source, tag, data):
        # Update heartbeat timestamp for any message from crawler
        self.crawler_status[source]['last_heartbeat'] = datetime.now()
        self.crawler_status[source]['active'] = True

        if tag == 1:  # Extracted URLs
            new_urls = set(data) - self.crawled_urls
            self.urls_to_crawl.update(new_urls)
            self.crawler_status[source]['urls_processed'] += 1
            logger.info(f"Received {len(new_urls)} new URLs from crawler {source}")
        
        elif tag == 99:  # Heartbeat
            logger.debug(f"Received heartbeat from crawler {source}")
        
        elif tag == 999:  # Error
            logger.error(f"Error from crawler {source}: {data}")

    def assign_tasks(self):
        available_crawlers = [node for node, status in self.crawler_status.items() 
                            if status['active']]
        
        for crawler in available_crawlers:
            urls_assigned = 0
            while urls_assigned < self.url_batch_size and self.urls_to_crawl:
                url = self.urls_to_crawl.pop()
                self.crawled_urls.add(url)
                self.comm.send(url, dest=crawler, tag=0)
                logger.info(f"Assigned URL {url} to crawler {crawler}")
                urls_assigned += 1

def display_status(master, crawler_status, indexer_status, urls_queue):
    """Display system status"""
    logger.info("\n=== System Status ===")
    logger.info(f"URLs Queue Size: {len(urls_queue)}")
    logger.info(f"Total URLs Crawled: {sum(status['processed'] for status in crawler_status.values())}")
    logger.info("\nCrawler Status:")
    for node, status in crawler_status.items():
        time_since_heartbeat = time.time() - status['last_heartbeat']
        status_symbol = "🟢" if time_since_heartbeat < 20 else "🔴"
        logger.info(f"Crawler {node}: {status_symbol} ACTIVE")
        logger.info(f"  ├─ URLs Processed: {status['processed']}")
        logger.info(f"  └─ Last Heartbeat: {int(time_since_heartbeat)}s ago")
    
    logger.info("\nSystem Health:")
    logger.info(f"Active Crawlers: {len(crawler_status)}/{len(master.active_crawler_nodes)}")
    logger.info(f"Processing Rate: {sum(status['processed'] for status in crawler_status.values()) / 60:.2f} URLs/minute")
    logger.info("==================")

def master_process():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    
    if size < 3:
        logger.error("Not enough nodes. Need at least 3 nodes (1 master, 1 crawler, 1 indexer)")
        return
    
    logger.name = 'Master'
    logger.info(f"Master node started with rank {rank} of {size}")
    
    # Initialize MasterNode
    master = MasterNode(comm, rank, size)
    master.initialize_seed_urls()
    
    logger.info(f"Active Crawler Nodes: {master.active_crawler_nodes}")
    logger.info(f"Active Indexer Nodes: {master.active_indexer_nodes}")
    
    # Initialize crawler and indexer status
    crawler_status = {node: {'processed': 0, 'last_heartbeat': time.time()} for node in master.active_crawler_nodes}
    indexer_status = {node: {'last_heartbeat': time.time()} for node in master.active_indexer_nodes}
    
    # Main loop
    last_status_time = time.time()
    while True:
        current_time = time.time()
        
        # Display status every 10 seconds
        if current_time - last_status_time >= 10:
            display_status(master, crawler_status, indexer_status, master.urls_to_crawl)
            last_status_time = current_time
        
        # Check for messages from crawlers and indexer
        if comm.Iprobe(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG):
            status = MPI.Status()
            message = comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
            
            source = status.Get_source()
            tag = status.Get_tag()
            
            if tag == 99:  # Heartbeat
                if source in crawler_status:
                    crawler_status[source]['last_heartbeat'] = time.time()
                elif source in indexer_status:
                    indexer_status[source]['last_heartbeat'] = time.time()
            elif tag == 100:  # Processed URLs count
                if source in crawler_status:
                    crawler_status[source]['processed'] = message
            elif tag == 1:  # New URLs from crawler
                if isinstance(message, list):
                    master.urls_to_crawl.update(message)
        
        # Assign URLs to crawlers
        master.assign_tasks()
        
        time.sleep(0.1)  # Small delay to prevent CPU overuse

if __name__ == '__main__':
    master_process() 