from mpi4py import MPI
import time
import logging
import requests
from bs4 import BeautifulSoup
import boto3
from urllib.parse import urljoin, urlparse
import os
from dotenv import load_dotenv
import json

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('Crawler')

# AWS Configuration
AWS_REGION = os.getenv('AWS_REGION', 'eu-north-1')
SQS_QUEUE_URL = os.getenv('SQS_QUEUE_URL')
sqs_client = boto3.client('sqs', region_name=AWS_REGION)

class Crawler:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        self.crawl_delay = 2  # Basic politeness measure

    def fetch_page(self, url):
        try:
            response = requests.get(url, headers=self.headers, timeout=10)
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            logger.error(f"Error fetching {url}: {e}")
            return None

    def extract_urls(self, html, base_url):
        soup = BeautifulSoup(html, 'html.parser')
        urls = []
        
        for link in soup.find_all('a', href=True):
            href = link['href']
            absolute_url = urljoin(base_url, href)
            if self.is_valid_url(absolute_url):
                urls.append(absolute_url)
        
        return urls

    def extract_text(self, html):
        soup = BeautifulSoup(html, 'html.parser')
        # Remove script and style elements
        for script in soup(["script", "style"]):
            script.decompose()
        return soup.get_text(separator=' ', strip=True)

    def is_valid_url(self, url):
        parsed = urlparse(url)
        return bool(parsed.netloc) and bool(parsed.scheme)

def crawler_process():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    
    logger.name = f'Crawler-{rank}'
    logger.info(f"Crawler node started with rank {rank}")
    crawler = Crawler()
    
    while True:
        # Receive URL from master
        status = MPI.Status()
        url_to_crawl = comm.recv(source=0, tag=0, status=status)
        
        if not url_to_crawl:
            logger.info(f"Crawler {rank} received shutdown signal. Exiting.")
            break

        logger.info(f"Crawler {rank} received URL: {url_to_crawl}")
        
        try:
            # Fetch and parse the page
            html = crawler.fetch_page(url_to_crawl)
            if html:
                # Extract URLs and text content
                extracted_urls = crawler.extract_urls(html, url_to_crawl)
                extracted_text = crawler.extract_text(html)
                
                # Send extracted URLs back to master
                comm.send(extracted_urls, dest=0, tag=1)
                
                # Send content to indexer via SQS
                message_body = {
                    'url': url_to_crawl,
                    'content': extracted_text,
                    'crawler_rank': rank
                }
                
                try:
                    sqs_client.send_message(
                        QueueUrl=SQS_QUEUE_URL,
                        MessageBody=json.dumps(message_body)
                    )
                    logger.info(f"Sent content to SQS for {url_to_crawl}")
                except Exception as e:
                    logger.error(f"Error sending to SQS: {e}")
                
                logger.info(f"Processed {url_to_crawl}, extracted {len(extracted_urls)} URLs")
            
            # Send heartbeat
            comm.send("alive", dest=0, tag=99)
            
            # Implement crawl delay
            time.sleep(crawler.crawl_delay)
            
        except Exception as e:
            logger.error(f"Error processing {url_to_crawl}: {e}")
            comm.send(f"Error processing {url_to_crawl}: {e}", dest=0, tag=999)

if __name__ == '__main__':
    crawler_process()    