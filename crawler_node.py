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
MAX_RETRIES = int(os.getenv('MAX_RETRIES', '3'))
HEALTH_CHECK_INTERVAL = int(os.getenv('HEALTH_CHECK_INTERVAL', '300'))  # 5 minutes

logger.info(f"Using SQS Queue URL: {SQS_QUEUE_URL}")
logger.info(f"Using SQS URLs Queue: {SQS_URLS_QUEUE}")
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
        self.retry_backoff = [2, 5, 10, 30, 60]  # Exponential backoff times in seconds
        self.last_health_check = time.time()
        self.stats = {
            "pages_crawled": 0,
            "failed_crawls": 0,
            "urls_extracted": 0,
            "message_retries": 0,
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
            "message_retries": self.stats["message_retries"],
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
            
            # Monitor queue health
            self.monitor_queue_health()
            
            logger.info("Health check: All services operational")
            return True
        except Exception as e:
            logger.error(f"Health check failed: {str(e)}")
            return False

    def handle_failed_message(self, message, error):
        """Process failed messages by marking and reinserting"""
        try:
            # Parse the original message
            if isinstance(message['Body'], str):
                body = json.loads(message['Body'])
            else:
                body = message['Body']
                
            # Add error information and retry count
            if 'error_count' not in body:
                body['error_count'] = 1
            else:
                body['error_count'] += 1
                
            body['last_error'] = str(error)
            body['last_retry'] = datetime.now().isoformat()
            
            # If max retries not exceeded, send back to original queue
            if body['error_count'] <= MAX_RETRIES:
                target_queue = SQS_URLS_QUEUE if body.get('message_type') == 'URL_TO_CRAWL' else SQS_QUEUE_URL
                delay_seconds = min(body['error_count'] * 60, 900)  # Backoff with delay, max 15 minutes
                
                sqs_client.send_message(
                    QueueUrl=target_queue,
                    MessageBody=json.dumps(body),
                    DelaySeconds=delay_seconds
                )
                logger.info(f"Requeued message for retry {body['error_count']}/{MAX_RETRIES} with delay {delay_seconds}s")
                self.stats["message_retries"] += 1
                return True
            else:
                logger.error(f"Message failed after {MAX_RETRIES} retries, dropping: {body}")
                return False
        except Exception as e:
            logger.error(f"Error handling failed message: {str(e)}")
            return False

    def monitor_queue_health(self):
        """Monitor queue for signs of processing issues"""
        try:
            # Check URLs queue
            url_response = sqs_client.get_queue_attributes(
                QueueUrl=SQS_URLS_QUEUE,
                AttributeNames=['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible']
            )
            
            # Check content queue
            content_response = sqs_client.get_queue_attributes(
                QueueUrl=SQS_QUEUE_URL,
                AttributeNames=['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible']
            )
            
            # Calculate health ratios
            url_visible = int(url_response['Attributes']['ApproximateNumberOfMessages'])
            url_in_flight = int(url_response['Attributes']['ApproximateNumberOfMessagesNotVisible'])
            
            content_visible = int(content_response['Attributes']['ApproximateNumberOfMessages'])
            content_in_flight = int(content_response['Attributes']['ApproximateNumberOfMessagesNotVisible'])
            
            # Check for high in-flight ratio which might indicate stuck processing
            if url_visible + url_in_flight > 0:
                url_health_ratio = url_in_flight / (url_visible + url_in_flight)
                if url_health_ratio > 0.8:  # More than 80% of messages are in-flight
                    logger.warning(f"URL queue health warning: {url_in_flight} in-flight vs {url_visible} visible messages")
            
            if content_visible + content_in_flight > 0:
                content_health_ratio = content_in_flight / (content_visible + content_in_flight)
                if content_health_ratio > 0.8:  # More than 80% of messages are in-flight
                    logger.warning(f"Content queue health warning: {content_in_flight} in-flight vs {content_visible} visible messages")
            
            return True
        except Exception as e:
            logger.error(f"Error monitoring queue health: {str(e)}")
            return False

    def process_url(self, url, message_body=None):
        if url in self.visited_urls:
            logger.info(f"Already visited {url}, skipping")
            return True

        logger.info(f"Processing URL: {url}")
        self.visited_urls.add(url)
        
        # Handle retries if this is a retry message
        if message_body and message_body.get('error_count', 0) > 0:
            logger.info(f"Processing retry {message_body['error_count']}/{MAX_RETRIES} for {url}")
        
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
                    'timestamp': datetime.now().isoformat()
                }
                
                try:
                    sqs_client.send_message(
                        QueueUrl=SQS_QUEUE_URL,
                        MessageBody=json.dumps(message_body)
                    )
                    logger.info(f"Sent content to indexer queue: {url}")
                    return True
                except Exception as e:
                    logger.error(f"Error sending content to indexer queue: {str(e)}")
                    return False
        return False

# Main crawler loop
def crawler_process():
    logger.info("Crawler node started")
    crawler = Crawler()
    
    # Initial health check
    crawler.health_check()
    
    last_health_check = time.time()
    
    while True:
        try:
            # Perform health check periodically
            if time.time() - last_health_check > HEALTH_CHECK_INTERVAL:
                crawler.health_check()
                last_health_check = time.time()
            
            # Poll for messages from the URL queue with long polling
            response = sqs_client.receive_message(
                QueueUrl=SQS_URLS_QUEUE,
                AttributeNames=['All'],
                MaxNumberOfMessages=1,
                WaitTimeSeconds=20
            )
            
            if 'Messages' in response:
                message = response['Messages'][0]
                receipt_handle = message['ReceiptHandle']
                
                try:
                    # Process the message
                    body = json.loads(message['Body'])
                    url = body.get('url')
                    
                    if url:
                        success = crawler.process_url(url, body)
                        if success:
                            # Delete message from queue only on success
                            sqs_client.delete_message(
                                QueueUrl=SQS_URLS_QUEUE,
                                ReceiptHandle=receipt_handle
                            )
                        else:
                            # Handle the failure case
                            crawler.handle_failed_message(message, "Failed to process URL")
                    else:
                        logger.warning("Received message without URL")
                        # Delete malformed message
                        sqs_client.delete_message(
                            QueueUrl=SQS_URLS_QUEUE,
                            ReceiptHandle=receipt_handle
                        )
                        
                except Exception as e:
                    logger.error(f"Error processing message: {str(e)}")
                    # Handle the exception
                    crawler.handle_failed_message(message, str(e))
            else:
                logger.info("No messages in queue, sleeping...")
                time.sleep(5)
                
        except Exception as e:
            logger.error(f"Error in crawler process: {str(e)}")
            time.sleep(5)

if __name__ == "__main__":
    crawler_process()    