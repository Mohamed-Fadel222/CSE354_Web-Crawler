from mpi4py import MPI
import time
import logging
import boto3
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('Process')

# AWS Configuration
AWS_REGION = os.getenv('AWS_REGION', 'eu-north-1')
SQS_QUEUE_URL = os.getenv('SQS_QUEUE_URL')
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
        self.crawler_status = {node: {'last_heartbeat': datetime.now(), 'active': True} 
                             for node in self.active_crawler_nodes}
        self.heartbeat_timeout = timedelta(seconds=30)

    def initialize_seed_urls(self):
        seed_urls = [
            "https://example.com",
            "https://example.org",
            "https://example.net"
        ]
        self.urls_to_crawl.update(seed_urls)
        logger.info(f"Initialized with seed URLs: {seed_urls}")

    def check_crawler_health(self):
        current_time = datetime.now()
        for node, status in self.crawler_status.items():
            if current_time - status['last_heartbeat'] > self.heartbeat_timeout:
                if status['active']:
                    logger.warning(f"Crawler node {node} has not sent heartbeat in {self.heartbeat_timeout}")
                    status['active'] = False

    def process_crawler_message(self, source, tag, data):
        if tag == 1:  # Extracted URLs
            new_urls = set(data) - self.crawled_urls
            self.urls_to_crawl.update(new_urls)
            logger.info(f"Received {len(new_urls)} new URLs from crawler {source}")
        
        elif tag == 99:  # Heartbeat
            self.crawler_status[source]['last_heartbeat'] = datetime.now()
            self.crawler_status[source]['active'] = True
            logger.debug(f"Received heartbeat from crawler {source}")
        
        elif tag == 999:  # Error
            logger.error(f"Error from crawler {source}: {data}")

    def assign_tasks(self):
        available_crawlers = [node for node, status in self.crawler_status.items() 
                            if status['active']]
        
        for crawler in available_crawlers:
            if self.urls_to_crawl:
                url = self.urls_to_crawl.pop()
                self.crawled_urls.add(url)
                self.comm.send(url, dest=crawler, tag=0)
                logger.info(f"Assigned URL {url} to crawler {crawler}")

def crawler_process(comm, rank):
    logger.name = f'Crawler-{rank}'
    logger.info(f"Crawler node started with rank {rank}")
    # Crawler logic will be implemented in crawler_node.py
    pass

def indexer_process(comm, rank):
    logger.name = f'Indexer-{rank}'
    logger.info(f"Indexer node started with rank {rank}")
    # Indexer logic will be implemented in indexer_node.py
    pass

def main():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    
    if size < 3:  # Need at least 1 master, 1 crawler, and 1 indexer
        logger.error("Not enough nodes. Need at least 3 nodes (1 master, 1 crawler, 1 indexer)")
        return

    # Set process-specific logger name
    if rank == 0:
        logger.name = 'Master'
        master = MasterNode(comm, rank, size)
        master.initialize_seed_urls()
        
        logger.info(f"Master node started with rank {rank} of {size}")
        logger.info(f"Active Crawler Nodes: {master.active_crawler_nodes}")
        logger.info(f"Active Indexer Nodes: {master.active_indexer_nodes}")

        while True:
            # Check for messages from crawlers
            if comm.iprobe(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG):
                status = MPI.Status()
                data = comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
                master.process_crawler_message(status.Get_source(), status.Get_tag(), data)

            # Check crawler health
            master.check_crawler_health()

            # Assign new tasks
            master.assign_tasks()

            time.sleep(1)  # Prevent CPU overuse
    
    elif rank <= size - 2:  # Crawler nodes
        crawler_process(comm, rank)
    else:  # Indexer node
        indexer_process(comm, rank)

if __name__ == '__main__':
    main() 