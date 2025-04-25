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
import urllib3
import warnings
from threading import Thread, Event

# Suppress warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings('ignore')

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('Crawler')

# AWS Configuration
AWS_REGION = os.getenv('AWS_REGION', 'eu-north-1')
SQS_QUEUE_URL = os.getenv('SQS_QUEUE_URL')
logger.info(f"Using SQS Queue URL: {SQS_QUEUE_URL}")
logger.info(f"Using AWS Region: {AWS_REGION}")
sqs_client = boto3.client('sqs', region_name=AWS_REGION)

class Crawler:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        self.crawl_delay = 1  # Reduced delay for testing
        self.visited_urls = set()
        self.session = requests.Session()
        self.session.verify = False  # Disable SSL verification
        self.session.headers.update(self.headers)
        self.session.timeout = 10  # Set timeout for all requests

    def fetch_page(self, url):
        try:
            logger.info(f"Attempting to fetch {url}")
            response = self.session.get(url)
            response.raise_for_status()
            logger.info(f"Successfully fetched {url}")
            return response.text
        except requests.RequestException as e:
            logger.error(f"Error fetching {url}: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error fetching {url}: {str(e)}")
            return None

    def extract_urls(self, html, base_url):
        if not html:
            return []
        
        try:
            soup = BeautifulSoup(html, 'html.parser')
            urls = []
            
            for link in soup.find_all('a', href=True):
                href = link['href']
                absolute_url = urljoin(base_url, href)
                if self.is_valid_url(absolute_url) and absolute_url not in self.visited_urls:
                    urls.append(absolute_url)
            
            logger.info(f"Extracted {len(urls)} new URLs from {base_url}")
            return urls[:10]  # Limit to 10 URLs per page to prevent overwhelming
        except Exception as e:
            logger.error(f"Error extracting URLs from {base_url}: {str(e)}")
            return []

    def extract_text(self, html):
        if not html:
            return ""
        
        try:
            soup = BeautifulSoup(html, 'html.parser')
            # Remove script and style elements
            for element in soup(["script", "style", "nav", "header", "footer"]):
                element.decompose()
            
            text = soup.get_text(separator=' ', strip=True)
            # Basic text cleaning
            text = ' '.join(text.split())
            logger.info(f"Extracted {len(text)} characters of text")
            return text
        except Exception as e:
            logger.error(f"Error extracting text: {str(e)}")
            return ""

    def is_valid_url(self, url):
        try:
            parsed = urlparse(url)
            return all([parsed.scheme, parsed.netloc]) and parsed.scheme in ['http', 'https']
        except Exception:
            return False

class HeartbeatThread(Thread):
    def __init__(self, comm, rank):
        super().__init__()
        self.comm = comm
        self.rank = rank
        self.stop_event = Event()
        self.daemon = True  # Thread will exit when main program exits

    def run(self):
        while not self.stop_event.is_set():
            try:
                self.comm.send("alive", dest=0, tag=99)
                logger.debug(f"Heartbeat sent from crawler {self.rank}")
            except Exception as e:
                logger.error(f"Error sending heartbeat: {str(e)}")
            time.sleep(10)  # Send heartbeat every 10 seconds

    def stop(self):
        self.stop_event.set()

def crawler_process():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    
    logger.name = f'Crawler-{rank}'
    logger.info(f"Crawler node started with rank {rank}")
    crawler = Crawler()
    
    # Start heartbeat thread
    heartbeat = HeartbeatThread(comm, rank)
    heartbeat.start()
    
    try:
        while True:
            try:
                # Receive URL from master
                status = MPI.Status()
                url_to_crawl = comm.recv(source=0, tag=0, status=status)
                
                if not url_to_crawl:
                    logger.info(f"Crawler {rank} received shutdown signal. Exiting.")
                    break

                if url_to_crawl in crawler.visited_urls:
                    logger.info(f"Already visited {url_to_crawl}, skipping")
                    continue

                logger.info(f"Processing URL: {url_to_crawl}")
                crawler.visited_urls.add(url_to_crawl)
                
                # Fetch and parse the page
                html = crawler.fetch_page(url_to_crawl)
                if html:
                    # Extract URLs and text content
                    extracted_urls = crawler.extract_urls(html, url_to_crawl)
                    extracted_text = crawler.extract_text(html)
                    
                    if extracted_text:
                        # Send extracted URLs back to master
                        comm.send(extracted_urls, dest=0, tag=1)
                        
                        # Send content to indexer via SQS
                        message_body = {
                            'url': url_to_crawl,
                            'content': extracted_text[:1000],  # Limit content size
                            'crawler_rank': rank
                        }
                        
                        try:
                            sqs_client.send_message(
                                QueueUrl=SQS_QUEUE_URL,
                                MessageBody=json.dumps(message_body)
                            )
                            logger.info(f"Sent content to SQS for {url_to_crawl}")
                        except Exception as e:
                            logger.error(f"Error sending to SQS: {str(e)}")
                
                # Implement crawl delay
                time.sleep(crawler.crawl_delay)
                
            except Exception as e:
                logger.error(f"Error in crawler process: {str(e)}")
                try:
                    comm.send(f"Error processing {url_to_crawl}: {str(e)}", dest=0, tag=999)
                except:
                    pass
                time.sleep(1)  # Wait before continuing
    finally:
        # Clean up
        heartbeat.stop()
        heartbeat.join()

if __name__ == '__main__':
    crawler_process()    