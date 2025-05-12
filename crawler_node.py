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
import uuid

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
logger.info(f"Using SQS Queue URL: {SQS_QUEUE_URL}")
logger.info(f"Using AWS Region: {AWS_REGION}")
sqs_client = boto3.client('sqs', region_name=AWS_REGION)

# Message Tags
MSG_TAG_INFO = 0       # Regular informational messages
MSG_TAG_URL = 1        # URL processing messages
MSG_TAG_CONTENT = 2    # Content processing messages  
MSG_TAG_WARNING = 99   # Warning messages
MSG_TAG_ERROR = 999    # Error messages

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
        self.crawler_id = str(uuid.uuid4())[:8]  # Generate a short unique ID for this crawler
        logger.info(f"Crawler initialized with ID: {self.crawler_id}")
        
        # Domain crawl limits - track URLs per domain
        self.domain_url_counts = {}
        self.max_urls_per_domain = 50  # Increased from 10 to 50 to ensure more complete domain coverage
        
    def should_crawl_url(self, url):
        """Check if we should crawl this URL based on domain limits"""
        if url in self.visited_urls:
            return False
            
        domain = self.get_domain_from_url(url)
        if not domain:
            return False
            
        # Check if we've reached domain limit
        if domain in self.domain_url_counts and self.domain_url_counts[domain] >= self.max_urls_per_domain:
            logger.info(f"[Crawler-{self.crawler_id}] Skipping {url} - reached limit of {self.max_urls_per_domain} URLs for domain {domain}")
            return False
            
        return True

    def fetch_page(self, url):
        try:
            logger.info(f"[Crawler-{self.crawler_id}] Attempting to fetch {url}")
            response = self.session.get(url)
            response.raise_for_status()
            logger.info(f"[Crawler-{self.crawler_id}] Successfully fetched {url} ({len(response.text)} bytes)")
            return response.text
        except requests.RequestException as e:
            logger.error(f"[Crawler-{self.crawler_id}] Error fetching {url}: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"[Crawler-{self.crawler_id}] Unexpected error fetching {url}: {str(e)}")
            return None

    def extract_urls(self, html, base_url):
        if not html:
            return []
        
        try:
            soup = BeautifulSoup(html, 'html.parser')
            urls = []
            base_domain = urlparse(base_url).netloc
            
            # Find all links
            all_links = soup.find_all('a', href=True)
            logger.info(f"[Crawler-{self.crawler_id}] Found {len(all_links)} total links on {base_url}")
            
            for link in all_links:
                href = link['href']
                # Make sure we handle relative URLs properly
                absolute_url = urljoin(base_url, href)
                parsed_url = urlparse(absolute_url)
                
                # Only follow URLs from the same domain
                if (parsed_url.netloc == base_domain and 
                    self.is_valid_url(absolute_url) and 
                    absolute_url not in self.visited_urls):
                    # Special handling for important paths we want to ensure are crawled
                    # We prioritize key paths like webinars, blog, documentation, etc.
                    priority_paths = ['/events/', '/webinars/', '/faq/', '/blog/', '/about/', '/documentation/']
                    is_priority = any(path in parsed_url.path for path in priority_paths)
                    
                    if is_priority:
                        # Insert priority URLs at the beginning to ensure they're crawled
                        urls.insert(0, absolute_url)
                        logger.info(f"[Crawler-{self.crawler_id}] Found priority URL: {absolute_url}")
                    else:
                        urls.append(absolute_url)
            
            # Ensure we're keeping enough URLs to reach important content
            # but still respecting a reasonable limit
            original_count = len(urls)
            urls = urls[:20]  # Increased from 10 to 20
            logger.info(f"[Crawler-{self.crawler_id}] Extracted {original_count} internal URLs from {base_url}, keeping {len(urls)} max")
            
            # Log the actual URLs being returned
            for idx, url in enumerate(urls):
                logger.info(f"[Crawler-{self.crawler_id}] URL {idx+1}: {url}")
                
            return urls
        except Exception as e:
            logger.error(f"[Crawler-{self.crawler_id}] Error extracting URLs from {base_url}: {str(e)}")
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
            logger.info(f"[Crawler-{self.crawler_id}] Extracted {len(text)} characters of text")
            return text
        except Exception as e:
            logger.error(f"[Crawler-{self.crawler_id}] Error extracting text: {str(e)}")
            return ""

    def is_valid_url(self, url):
        try:
            parsed = urlparse(url)
            return all([parsed.scheme, parsed.netloc]) and parsed.scheme in ['http', 'https']
        except Exception:
            return False

    def get_domain_from_url(self, url):
        try:
            return urlparse(url).netloc
        except:
            return None

    def process_url(self, url):
        """
        Process a URL by crawling it and sending discovered URLs to the queue
        """
        # Check if we should crawl this URL
        domain = self.get_domain_from_url(url)
        if not domain:
            logger.warning(f"[Crawler-{self.crawler_id}] Invalid domain for URL: {url}")
            return
            
        # Skip if already visited
        if url in self.visited_urls:
            logger.info(f"[Crawler-{self.crawler_id}] Already visited {url}, skipping")
            return
            
        # Check domain limits
        if domain in self.domain_url_counts and self.domain_url_counts[domain] >= self.max_urls_per_domain:
            logger.info(f"[Crawler-{self.crawler_id}] Skipping {url} - reached limit of {self.max_urls_per_domain} URLs for domain {domain}")
            return

        logger.info(f"[Crawler-{self.crawler_id}] Processing URL: {url}")
        self.visited_urls.add(url)
        
        # Update domain counter
        self.domain_url_counts[domain] = self.domain_url_counts.get(domain, 0) + 1
        logger.info(f"[Crawler-{self.crawler_id}] Domain {domain}: {self.domain_url_counts[domain]}/{self.max_urls_per_domain} URLs processed")
        
        # Fetch and parse the page
        html = self.fetch_page(url)
        if html:
            # Extract URLs and text content
            extracted_urls = self.extract_urls(html, url)
            extracted_text = self.extract_text(html)
            
            if extracted_text:
                # Send content to indexer queue
                message_body = {
                    'tag': MSG_TAG_CONTENT,  # Tag for content messages
                    'url': url,
                    'content': extracted_text[:5000],  # Increased content size
                    'timestamp': time.time()
                }
                
                try:
                    response = sqs_client.send_message(
                        QueueUrl=SQS_QUEUE_URL,
                        MessageBody=json.dumps(message_body)
                    )
                    logger.info(f"[Crawler-{self.crawler_id}] Sent content to indexer queue for {url} (MessageId: {response.get('MessageId')})")
                except Exception as e:
                    logger.error(f"[Crawler-{self.crawler_id}] Error sending to indexer queue: {str(e)}")
            
            # Queue all discovered URLs to the URLs queue (letting master node handle them)
            if extracted_urls:
                urls_queued = 0
                for new_url in extracted_urls:
                    new_domain = self.get_domain_from_url(new_url)
                    
                    # Only queue if we haven't hit the domain limit yet
                    current_count = self.domain_url_counts.get(new_domain, 0)
                    if current_count < self.max_urls_per_domain:
                        try:
                            response = sqs_client.send_message(
                                QueueUrl=SQS_URLS_QUEUE,
                                MessageBody=json.dumps({
                                    'tag': MSG_TAG_URL,  # Tag for URL messages
                                    'url': new_url,
                                    'source_url': url,
                                    'timestamp': time.time()
                                })
                            )
                            urls_queued += 1
                            logger.info(f"[Crawler-{self.crawler_id}] Queued new URL for crawling: {new_url} (MessageId: {response.get('MessageId')})")
                        except Exception as e:
                            logger.error(f"[Crawler-{self.crawler_id}] Error queuing URL {new_url}: {str(e)}")
                    else:
                        logger.info(f"[Crawler-{self.crawler_id}] Skipping {new_url} - domain {new_domain} reached limit of {self.max_urls_per_domain}")
                
                logger.info(f"[Crawler-{self.crawler_id}] SUCCESS: Queued {urls_queued} URLs from {url} for further crawling")
                            
        # Implement crawl delay
        time.sleep(self.crawl_delay)

    def test_extract_and_queue(self, url):
        """
        Test function to extract URLs from a page and print them,
        but not actually queue them. Useful for debugging.
        """
        logger.info(f"[TEST] Testing URL extraction on {url}")
        html = self.fetch_page(url)
        if html:
            urls = self.extract_urls(html, url)
            logger.info(f"[TEST] Found {len(urls)} URLs to crawl on {url}")
            for i, extracted_url in enumerate(urls):
                logger.info(f"[TEST] URL {i+1}: {extracted_url}")
        else:
            logger.error(f"[TEST] Could not fetch {url}")

def crawler_process():
    logger.info("*** Crawler node started - Processing URLs and sending discovered URLs to queue ***")
    logger.info("*** FLOW: 1) Pull URLs from queue → 2) Crawl each URL → 3) Extract links → 4) Send links back to queue ***")
    crawler = Crawler()
    
    # Optional: Test URL extraction on a specific URL
    # crawler.test_extract_and_queue("https://example.com")
    
    while True:
        try:
            # Log that we're waiting for messages
            logger.info(f"[Crawler-{crawler.crawler_id}] Waiting for URLs from the queue...")
            
            # Receive URL from SQS
            response = sqs_client.receive_message(
                QueueUrl=SQS_URLS_QUEUE,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=20,
                VisibilityTimeout=30
            )

            if 'Messages' in response:
                msg_count = len(response['Messages'])
                logger.info(f"[Crawler-{crawler.crawler_id}] Received {msg_count} message(s) from queue")
                
                for message in response['Messages']:
                    receipt_handle = message['ReceiptHandle']
                    message_id = message.get('MessageId', 'unknown')
                    
                    try:
                        body = json.loads(message['Body'])
                        url = body['url']
                        tag = body.get('tag', MSG_TAG_URL)  # Default to URL tag if not specified
                        
                        logger.info(f"[Crawler-{crawler.crawler_id}] Received URL from queue: {url} (MessageId: {message_id}, Tag: {tag})")
                        
                        # Process the URL (fetches content and queues discovered URLs)
                        crawler.process_url(url)
                        
                        # Delete the message from the queue after successful processing
                        sqs_client.delete_message(
                            QueueUrl=SQS_URLS_QUEUE,
                            ReceiptHandle=receipt_handle
                        )
                        logger.info(f"[Crawler-{crawler.crawler_id}] Successfully processed and deleted message for {url}")
                        
                    except json.JSONDecodeError as e:
                        # Send error message to queue
                        try:
                            sqs_client.send_message(
                                QueueUrl=SQS_QUEUE_URL,
                                MessageBody=json.dumps({
                                    'tag': MSG_TAG_ERROR,
                                    'error': f"JSON decode error: {str(e)}",
                                    'message_id': message_id,
                                    'timestamp': time.time()
                                })
                            )
                        except Exception as send_err:
                            logger.error(f"[Crawler-{crawler.crawler_id}] Error sending error message: {str(send_err)}")
                            
                        logger.error(f"[Crawler-{crawler.crawler_id}] Error decoding message: {str(e)}")
                        # Delete malformed messages
                        sqs_client.delete_message(
                            QueueUrl=SQS_URLS_QUEUE,
                            ReceiptHandle=receipt_handle
                        )
                    except Exception as e:
                        # Send error message to queue
                        try:
                            sqs_client.send_message(
                                QueueUrl=SQS_QUEUE_URL,
                                MessageBody=json.dumps({
                                    'tag': MSG_TAG_ERROR,
                                    'error': f"Processing error: {str(e)}",
                                    'url': body.get('url', 'unknown'),
                                    'message_id': message_id,
                                    'timestamp': time.time()
                                })
                            )
                        except Exception as send_err:
                            logger.error(f"[Crawler-{crawler.crawler_id}] Error sending error message: {str(send_err)}")
                            
                        logger.error(f"[Crawler-{crawler.crawler_id}] Error processing message: {str(e)}")
                        # Return the message to the queue for retry
                        try:
                            sqs_client.change_message_visibility(
                                QueueUrl=SQS_URLS_QUEUE,
                                ReceiptHandle=receipt_handle,
                                VisibilityTimeout=0  # Make immediately visible for retry
                            )
                            logger.info(f"[Crawler-{crawler.crawler_id}] Returned message to queue for retry")
                        except Exception as e:
                            logger.error(f"[Crawler-{crawler.crawler_id}] Error changing message visibility: {str(e)}")
            else:
                logger.info(f"[Crawler-{crawler.crawler_id}] No messages received from queue")
            
            # Small delay between queue checks
            time.sleep(2)
            
        except Exception as e:
            logger.error(f"[Crawler-{crawler.crawler_id}] Error in crawler process: {str(e)}")
            time.sleep(5)  # Wait before retrying

if __name__ == '__main__':
    crawler_process()    