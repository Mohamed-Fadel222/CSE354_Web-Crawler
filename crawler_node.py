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
import random
from datetime import datetime

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
SQS_URLS_QUEUE = os.getenv('SQS_URLS_QUEUE')  # Queue for URLs to crawl
SQS_DLQ_URL = os.getenv('SQS_DLQ_URL', '')  # Dead Letter Queue
MAX_RETRIES = int(os.getenv('MAX_RETRIES', '3'))
HEALTH_CHECK_INTERVAL = int(os.getenv('HEALTH_CHECK_INTERVAL', '300'))  # 5 minutes

logger.info(f"Using SQS Queue URL: {SQS_QUEUE_URL}")
logger.info(f"Using SQS URLs Queue: {SQS_URLS_QUEUE}")
logger.info(f"Using AWS Region: {AWS_REGION}")
if SQS_DLQ_URL:
    logger.info(f"Using Dead Letter Queue: {SQS_DLQ_URL}")

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
        self.retry_backoff = [2, 5, 10, 30, 60]  # Exponential backoff times in seconds
        self.last_health_check = time.time()
        self.stats = {
            "pages_crawled": 0,
            "failed_crawls": 0,
            "urls_extracted": 0,
            "start_time": datetime.now()
        }

    def fetch_page(self, url):
        retry_count = 0
        while retry_count < MAX_RETRIES:
            try:
                logger.info(f"Attempting to fetch {url}")
                response = self.session.get(url)
                response.raise_for_status()
                logger.info(f"Successfully fetched {url}")
                self.stats["pages_crawled"] += 1
                return response.text
            except requests.RequestException as e:
                retry_count += 1
                if retry_count >= MAX_RETRIES:
                    logger.error(f"Failed to fetch {url} after {MAX_RETRIES} attempts: {str(e)}")
                    self.stats["failed_crawls"] += 1
                    return None
                
                backoff_time = self.retry_backoff[min(retry_count-1, len(self.retry_backoff)-1)]
                logger.warning(f"Error fetching {url}, retry {retry_count}/{MAX_RETRIES} in {backoff_time}s: {str(e)}")
                time.sleep(backoff_time)
            except Exception as e:
                logger.error(f"Unexpected error fetching {url}: {str(e)}")
                self.stats["failed_crawls"] += 1
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
            
            extracted_count = len(urls[:10])  # Limit to 10 URLs per page
            self.stats["urls_extracted"] += extracted_count
            logger.info(f"Extracted {extracted_count} new URLs from {base_url}")
            return urls[:10]  # Limit to 10 URLs per page to prevent overwhelming
        except Exception as e:
            logger.error(f"Error extracting URLs from {base_url}: {str(e)}")
            return []

    def extract_text(self, html):
        if not html:
            return "", ""
        
        try:
            soup = BeautifulSoup(html, 'html.parser')
            # Remove script and style elements
            for element in soup(["script", "style", "nav", "header", "footer"]):
                element.decompose()
            
            # Try to get the title
            title = soup.title.string if soup.title else ""
            
            text = soup.get_text(separator=' ', strip=True)
            # Basic text cleaning
            text = ' '.join(text.split())
            logger.info(f"Extracted {len(text)} characters of text")
            return text, title
        except Exception as e:
            logger.error(f"Error extracting text: {str(e)}")
            return "", ""

    def is_valid_url(self, url):
        try:
            parsed = urlparse(url)
            return all([parsed.scheme, parsed.netloc]) and parsed.scheme in ['http', 'https']
        except Exception:
            return False

    def get_stats(self):
        """Return crawler statistics"""
        uptime = (datetime.now() - self.stats["start_time"]).total_seconds()
        pages_per_minute = 0
        if uptime > 0:
            pages_per_minute = (self.stats["pages_crawled"] / (uptime / 60))
            
        return {
            "pages_crawled": self.stats["pages_crawled"],
            "failed_crawls": self.stats["failed_crawls"],
            "urls_extracted": self.stats["urls_extracted"],
            "uptime_seconds": int(uptime),
            "crawl_rate": round(pages_per_minute, 2),
            "last_health_check": time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(self.last_health_check))
        }

    def health_check(self):
        """Perform a health check"""
        self.last_health_check = time.time()
        
        try:
            # Check SQS connections
            sqs_client.get_queue_attributes(
                QueueUrl=SQS_URLS_QUEUE,
                AttributeNames=['ApproximateNumberOfMessages']
            )
            
            sqs_client.get_queue_attributes(
                QueueUrl=SQS_QUEUE_URL,
                AttributeNames=['ApproximateNumberOfMessages']
            )
            
            # Check network connectivity by fetching a reliable page
            test_url = "http://www.example.com"
            response = self.session.get(test_url, timeout=5)
            response.raise_for_status()
            
            logger.info("Health check: All services operational")
            return True
        except Exception as e:
            logger.error(f"Health check failed: {str(e)}")
            return False

    def send_to_dlq(self, message, error):
        """Send failed message to Dead Letter Queue"""
        if not SQS_DLQ_URL:
            logger.warning("No Dead Letter Queue configured, dropping failed message")
            return False
            
        try:
            # Add error information to the message
            if isinstance(message['Body'], str):
                body = json.loads(message['Body'])
            else:
                body = message['Body']
                
            dlq_message = {
                "original_message": body,
                "error": str(error),
                "timestamp": datetime.now().isoformat(),
                "message_type": "URL_PROCESSING_FAILED"
            }
            
            sqs_client.send_message(
                QueueUrl=SQS_DLQ_URL,
                MessageBody=json.dumps(dlq_message)
            )
            logger.info(f"Sent failed message to DLQ: {body.get('url', 'unknown URL')}")
            return True
        except Exception as e:
            logger.error(f"Error sending to DLQ: {str(e)}")
            return False

    def process_url(self, url):
        if url in self.visited_urls:
            logger.info(f"Already visited {url}, skipping")
            return True

        logger.info(f"Processing URL: {url}")
        self.visited_urls.add(url)
        
        # Fetch and parse the page
        html = self.fetch_page(url)
        if html:
            # Extract URLs and text content
            extracted_urls = self.extract_urls(html, url)
            extracted_text, title = self.extract_text(html)
            
            if extracted_text:
                # Send extracted URLs to the URLs queue
                for new_url in extracted_urls:
                    try:
                        sqs_client.send_message(
                            QueueUrl=SQS_URLS_QUEUE,
                            MessageBody=json.dumps({
                                'url': new_url, 
                                'message_type': 'URL_TO_CRAWL',
                                'source_url': url,
                                'timestamp': datetime.now().isoformat()
                            })
                        )
                        logger.info(f"Sent new URL to queue: {new_url}")
                    except Exception as e:
                        logger.error(f"Error sending URL to queue: {str(e)}")
                
                # Send content to indexer queue
                message_body = {
                    'url': url,
                    'content': extracted_text[:1000],  # Limit content size
                    'title': title[:200] if title else "",  # Add title information
                    'message_type': 'CONTENT_TO_INDEX',
                    'crawl_timestamp': datetime.now().isoformat(),
                    'extracted_urls_count': len(extracted_urls)
                }
                
                retry_count = 0
                while retry_count < MAX_RETRIES:
                    try:
                        sqs_client.send_message(
                            QueueUrl=SQS_QUEUE_URL,
                            MessageBody=json.dumps(message_body)
                        )
                        logger.info(f"Sent content to SQS for {url}")
                        return True
                    except Exception as e:
                        retry_count += 1
                        if retry_count >= MAX_RETRIES:
                            logger.error(f"Failed to send content to SQS after {MAX_RETRIES} attempts: {str(e)}")
                            return False
                        
                        backoff_time = self.retry_backoff[min(retry_count-1, len(self.retry_backoff)-1)]
                        logger.warning(f"Error sending to SQS, retry {retry_count}/{MAX_RETRIES} in {backoff_time}s: {str(e)}")
                        time.sleep(backoff_time)
            else:
                logger.warning(f"No text content extracted from {url}")
                return False
        else:
            logger.error(f"Failed to fetch content from {url}")
            return False

def crawler_process():
    logger.info("Crawler node started")
    crawler = Crawler()
    
    # Log initial stats
    logger.info(f"Initial crawler stats: {crawler.get_stats()}")
    
    # Initial health check
    crawler.health_check()
    
    consecutive_empty = 0
    
    while True:
        try:
            # Periodic health check
            if time.time() - crawler.last_health_check > HEALTH_CHECK_INTERVAL:
                crawler.health_check()
            
            # Receive URL from SQS with adaptive wait time
            wait_time = 20  # Default long polling
            if consecutive_empty > 5:
                wait_time = min(20, consecutive_empty)  # Increase wait time when queue is empty
                
            response = sqs_client.receive_message(
                QueueUrl=SQS_URLS_QUEUE,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=wait_time,
                VisibilityTimeout=30
            )

            if 'Messages' in response:
                consecutive_empty = 0
                for message in response['Messages']:
                    try:
                        body = json.loads(message['Body'])
                        url = body['url']
                        
                        # Process the URL
                        if crawler.process_url(url):
                            # Success - delete the message
                            sqs_client.delete_message(
                                QueueUrl=SQS_URLS_QUEUE,
                                ReceiptHandle=message['ReceiptHandle']
                            )
                        else:
                            # Processing failed, send to DLQ
                            crawler.send_to_dlq(message, "URL processing failed")
                            
                            # Still delete from main queue
                            sqs_client.delete_message(
                                QueueUrl=SQS_URLS_QUEUE,
                                ReceiptHandle=message['ReceiptHandle']
                            )
                        
                    except json.JSONDecodeError as e:
                        logger.error(f"Error decoding message: {str(e)}")
                        crawler.send_to_dlq(message, f"JSON decode error: {str(e)}")
                    except KeyError as e:
                        logger.error(f"Missing key in message: {str(e)}")
                        crawler.send_to_dlq(message, f"Missing key: {str(e)}")
                    except Exception as e:
                        logger.error(f"Error processing message: {str(e)}")
                        crawler.send_to_dlq(message, f"General error: {str(e)}")
                    
                    # Always try to delete the message even if processing failed
                    try:
                        sqs_client.delete_message(
                            QueueUrl=SQS_URLS_QUEUE,
                            ReceiptHandle=message['ReceiptHandle']
                        )
                    except Exception as e:
                        logger.error(f"Error deleting message: {str(e)}")
            else:
                consecutive_empty += 1
                if consecutive_empty % 10 == 0:
                    logger.info(f"No messages received for {consecutive_empty} consecutive polls")
                # Add some jitter to prevent thundering herd with multiple crawlers
                time.sleep(random.uniform(0.5, crawler.crawl_delay * 1.5))
                continue
            
            # Add normal crawl delay
            time.sleep(crawler.crawl_delay)
            
        except Exception as e:
            logger.error(f"Error in crawler process: {str(e)}")
            time.sleep(5)  # Wait before retrying

if __name__ == '__main__':
    crawler_process()    