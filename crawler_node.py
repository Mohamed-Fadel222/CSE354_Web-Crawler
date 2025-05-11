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
import traceback
import socket
from datetime import datetime
import sys
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

# Log the message tag definitions to help with debugging
logger = logging.getLogger('Crawler')
logger.info("Message tag definitions:")
for tag_name in dir(MessageTags):
    if not tag_name.startswith('__'):
        tag_value = getattr(MessageTags, tag_name)
        logger.info(f"  {tag_name} = {tag_value}")

# Suppress warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings('ignore')

# Load environment variables
load_dotenv()

# Configure advanced logging
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()
numeric_level = getattr(logging, LOG_LEVEL, logging.INFO)

# Create logs directory if it doesn't exist
os.makedirs('logs', exist_ok=True)

# Configure logging to file and console with rotation
logging.basicConfig(
    level=numeric_level,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/crawler_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger('Crawler')

# Set Requests library logging to WARNING to reduce noise
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

# AWS Configuration
AWS_REGION = os.getenv('AWS_REGION', 'eu-north-1')
SQS_QUEUE_URL = os.getenv('SQS_QUEUE_URL')
SQS_URLS_QUEUE = os.getenv('SQS_URLS_QUEUE')  # Queue for URLs to crawl
SQS_DLQ_URL = os.getenv('SQS_DLQ_URL')  # Dead Letter Queue for failed messages

# Log important configuration
logger.info(f"Crawler starting with configuration:")
logger.info(f"  Log Level: {LOG_LEVEL}")
logger.info(f"  AWS Region: {AWS_REGION}")
logger.info(f"  Content Queue: {SQS_QUEUE_URL}")
logger.info(f"  URLs Queue: {SQS_URLS_QUEUE}")
logger.info(f"  Dead Letter Queue: {SQS_DLQ_URL if SQS_DLQ_URL else 'Not configured'}")

# Initialize AWS clients with retry configuration
try:
    session = boto3.Session(region_name=AWS_REGION)
    sqs_client = session.client('sqs', 
                               config=boto3.session.Config(
                                   retries={'max_attempts': 5, 'mode': 'standard'},
                                   connect_timeout=5,
                                   read_timeout=10
                               ))
    logger.info("AWS clients initialized successfully with retry configuration")
except Exception as e:
    logger.critical(f"Failed to initialize AWS clients: {str(e)}")
    logger.critical(traceback.format_exc())
    sys.exit(1)  # Exit if we can't connect to AWS as it's a critical dependency

class Crawler:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        self.crawl_delay = float(os.getenv('CRAWL_DELAY', '1.0'))  # Configurable delay
        self.visited_urls = set()
        self.session = requests.Session()
        self.session.verify = False  # Disable SSL verification
        self.session.headers.update(self.headers)
        self.session.timeout = int(os.getenv('REQUEST_TIMEOUT', '10'))  # Configurable timeout
        self.crawler_id = str(uuid.uuid4())[:8]  # Generate a short unique ID for this crawler
        self.hostname = socket.gethostname()  # Get hostname for tracking
        
        # Domain crawl limits - track URLs per domain
        self.domain_url_counts = {}
        self.max_urls_per_domain = int(os.getenv('MAX_URLS_PER_DOMAIN', '10'))  # Configurable limit
        
        # Circuit breaker pattern variables
        self.error_count = 0
        self.error_threshold = int(os.getenv('ERROR_THRESHOLD', '5'))
        self.backoff_time = int(os.getenv('BACKOFF_TIME', '60'))  # seconds
        self.last_backoff_time = None
        
        # Error tracking
        self.consecutive_errors = 0
        self.max_consecutive_errors = int(os.getenv('MAX_CONSECUTIVE_ERRORS', '3'))
        
        # Periodic state persistence
        self.crawled_count = 0
        self.state_save_interval = int(os.getenv('STATE_SAVE_INTERVAL', '50'))  # Save state every 50 URLs
        
        # Health metrics
        self.start_time = datetime.now()
        self.successful_requests = 0
        self.failed_requests = 0
        
        logger.info(f"Crawler initialized with ID: {self.crawler_id} on host {self.hostname}")
        logger.info(f"Crawler configuration: crawl_delay={self.crawl_delay}, max_urls_per_domain={self.max_urls_per_domain}")

    def save_state(self):
        """Save crawler state to file for potential recovery"""
        try:
            state = {
                'crawler_id': self.crawler_id,
                'visited_urls': list(self.visited_urls),
                'domain_url_counts': self.domain_url_counts,
                'timestamp': datetime.now().isoformat(),
                'stats': {
                    'crawled_count': self.crawled_count,
                    'successful_requests': self.successful_requests,
                    'failed_requests': self.failed_requests,
                    'uptime_seconds': (datetime.now() - self.start_time).total_seconds()
                }
            }
            
            with open(f'logs/crawler_state_{self.crawler_id}.json', 'w') as f:
                json.dump(state, f)
            
            logger.info(f"[Crawler-{self.crawler_id}] Saved state: {self.crawled_count} URLs crawled")
        except Exception as e:
            logger.error(f"[Crawler-{self.crawler_id}] Failed to save state: {str(e)}")

    def load_state(self, state_file):
        """Load crawler state from file for recovery"""
        try:
            if os.path.exists(state_file):
                with open(state_file, 'r') as f:
                    state = json.load(f)
                
                self.visited_urls = set(state['visited_urls'])
                self.domain_url_counts = state['domain_url_counts']
                self.crawled_count = state['stats']['crawled_count']
                
                logger.info(f"[Crawler-{self.crawler_id}] Loaded state from {state_file}: {len(self.visited_urls)} URLs in history")
                return True
            return False
        except Exception as e:
            logger.error(f"[Crawler-{self.crawler_id}] Failed to load state: {str(e)}")
            return False

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

    def circuit_breaker_check(self):
        """Implement circuit breaker pattern to prevent overloading servers"""
        if self.last_backoff_time:
            elapsed = (datetime.now() - self.last_backoff_time).total_seconds()
            if elapsed < self.backoff_time:
                logger.warning(f"[Crawler-{self.crawler_id}] Circuit breaker active, backing off for {self.backoff_time - elapsed:.1f} more seconds")
                return False
            else:
                # Reset after backoff period
                self.last_backoff_time = None
                self.error_count = 0
        
        if self.error_count >= self.error_threshold:
            logger.warning(f"[Crawler-{self.crawler_id}] Circuit breaker triggered after {self.error_count} errors. Backing off for {self.backoff_time} seconds")
            self.last_backoff_time = datetime.now()
            return False
        
        return True

    def fetch_page(self, url):
        """Fetch a web page with robust error handling and circuit breaker pattern"""
        if not self.circuit_breaker_check():
            return None
            
        try:
            logger.info(f"[Crawler-{self.crawler_id}] Attempting to fetch {url}")
            response = self.session.get(url, timeout=self.session.timeout)
            response.raise_for_status()
            
            # Success, reset consecutive errors
            self.consecutive_errors = 0
            self.successful_requests += 1
            
            logger.info(f"[Crawler-{self.crawler_id}] Successfully fetched {url} ({len(response.text)} bytes)")
            return response.text
            
        except requests.exceptions.Timeout as e:
            self.error_count += 1
            self.consecutive_errors += 1
            self.failed_requests += 1
            logger.warning(f"[Crawler-{self.crawler_id}] Timeout fetching {url}: {str(e)}")
            return None
        except requests.exceptions.TooManyRedirects as e:
            self.error_count += 1
            self.consecutive_errors += 1
            self.failed_requests += 1
            logger.warning(f"[Crawler-{self.crawler_id}] Too many redirects for {url}: {str(e)}")
            return None
        except requests.exceptions.ConnectionError as e:
            self.error_count += 1
            self.consecutive_errors += 1
            self.failed_requests += 1
            logger.warning(f"[Crawler-{self.crawler_id}] Connection error fetching {url}: {str(e)}")
            return None
        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code if e.response else "unknown"
            self.failed_requests += 1
            
            # Only increment error count for server errors, not client errors
            if status_code >= 500:
                self.error_count += 1
                self.consecutive_errors += 1
                logger.warning(f"[Crawler-{self.crawler_id}] HTTP error {status_code} fetching {url}: {str(e)}")
            else:
                logger.info(f"[Crawler-{self.crawler_id}] HTTP error {status_code} fetching {url}: {str(e)}")
            return None
        except Exception as e:
            self.error_count += 1
            self.consecutive_errors += 1
            self.failed_requests += 1
            logger.error(f"[Crawler-{self.crawler_id}] Unexpected error fetching {url}: {str(e)}")
            logger.debug(traceback.format_exc())
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
                try:
                    absolute_url = urljoin(base_url, href)
                    parsed_url = urlparse(absolute_url)
                    
                    # Only follow URLs from the same domain
                    if (parsed_url.netloc == base_domain and 
                        self.is_valid_url(absolute_url) and 
                        absolute_url not in self.visited_urls):
                        urls.append(absolute_url)
                except Exception as e:
                    logger.warning(f"[Crawler-{self.crawler_id}] Error processing URL {href}: {str(e)}")
                    continue
            
            # Limit to 10 URLs per page as requested
            original_count = len(urls)
            urls = urls[:10]
            logger.info(f"[Crawler-{self.crawler_id}] Extracted {original_count} internal URLs from {base_url}, keeping 10 max")
            
            # Log the actual URLs being returned
            for idx, url in enumerate(urls):
                logger.info(f"[Crawler-{self.crawler_id}] URL {idx+1}: {url}")
                
            return urls
        except Exception as e:
            logger.error(f"[Crawler-{self.crawler_id}] Error extracting URLs from {base_url}: {str(e)}")
            logger.debug(traceback.format_exc())
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
            logger.debug(traceback.format_exc())
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

    def send_to_dlq(self, message_body, error_details):
        """Send failed message to Dead Letter Queue"""
        if not SQS_DLQ_URL:
            logger.warning(f"[Crawler-{self.crawler_id}] No DLQ configured, discarding failed message")
            return False
            
        try:
            enriched_message = {
                'original_message': message_body,
                'error_details': error_details,
                'crawler_id': self.crawler_id,
                'hostname': self.hostname,
                'timestamp': datetime.now().isoformat(),
                'tag': MessageTags.STATUS_ERROR  # Add message tag
            }
            
            response = sqs_client.send_message(
                QueueUrl=SQS_DLQ_URL,
                MessageBody=json.dumps(enriched_message)
            )
            logger.info(f"[Crawler-{self.crawler_id}] Sent failed message to DLQ: {response.get('MessageId')}")
            return True
        except Exception as e:
            logger.error(f"[Crawler-{self.crawler_id}] Failed to send to DLQ: {str(e)}")
            return False

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

        # Check if we've had too many consecutive errors and need to back off
        if self.consecutive_errors >= self.max_consecutive_errors:
            logger.warning(f"[Crawler-{self.crawler_id}] Too many consecutive errors ({self.consecutive_errors}), backing off")
            time.sleep(min(5 * self.consecutive_errors, 60))  # Exponential backoff up to 60 seconds
        
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
                    'url': url,
                    'content': extracted_text[:5000],  # Increased content size
                    'timestamp': datetime.now().isoformat(),
                    'crawler_id': self.crawler_id,
                    'hostname': self.hostname,
                    'tag': MessageTags.CONTENT_TEXT  # Add message tag for content type
                }
                
                try:
                    response = sqs_client.send_message(
                        QueueUrl=SQS_QUEUE_URL,
                        MessageBody=json.dumps(message_body)
                    )
                    logger.info(f"[Crawler-{self.crawler_id}] Sent content to indexer queue for {url} (MessageId: {response.get('MessageId')}, Tag: {MessageTags.CONTENT_TEXT})")
                except Exception as e:
                    logger.error(f"[Crawler-{self.crawler_id}] Error sending to indexer queue: {str(e)}")
                    self.send_to_dlq(message_body, f"Failed to send to indexer queue: {str(e)}")
            
            # Queue all discovered URLs to the URLs queue (letting master node handle them)
            if extracted_urls:
                urls_queued = 0
                for new_url in extracted_urls:
                    new_domain = self.get_domain_from_url(new_url)
                    
                    # Only queue if we haven't hit the domain limit yet
                    current_count = self.domain_url_counts.get(new_domain, 0)
                    if current_count < self.max_urls_per_domain:
                        try:
                            message_body = {
                                'url': new_url,
                                'source_url': url,
                                'timestamp': datetime.now().isoformat(),
                                'crawler_id': self.crawler_id,
                                'tag': MessageTags.URL_NEW  # Add message tag for URL type
                            }
                            
                            response = sqs_client.send_message(
                                QueueUrl=SQS_URLS_QUEUE,
                                MessageBody=json.dumps(message_body)
                            )
                            urls_queued += 1
                            logger.info(f"[Crawler-{self.crawler_id}] Queued new URL for crawling: {new_url} (MessageId: {response.get('MessageId')}, Tag: {MessageTags.URL_NEW})")
                        except Exception as e:
                            logger.error(f"[Crawler-{self.crawler_id}] Error queuing URL {new_url}: {str(e)}")
                    else:
                        logger.info(f"[Crawler-{self.crawler_id}] Skipping {new_url} - domain {new_domain} reached limit of {self.max_urls_per_domain}")
                
                logger.info(f"[Crawler-{self.crawler_id}] SUCCESS: Queued {urls_queued} URLs from {url} for further crawling")
        
        # Update crawl count and periodically save state
        self.crawled_count += 1
        if self.crawled_count % self.state_save_interval == 0:
            self.save_state()
            
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
            
    def health_check(self):
        """Report crawler health metrics"""
        uptime = datetime.now() - self.start_time
        total_requests = self.successful_requests + self.failed_requests
        success_rate = self.successful_requests / max(total_requests, 1) * 100
        
        health_data = {
            'crawler_id': self.crawler_id,
            'hostname': self.hostname,
            'uptime_seconds': uptime.total_seconds(),
            'crawled_count': self.crawled_count,
            'successful_requests': self.successful_requests,
            'failed_requests': self.failed_requests,
            'success_rate_percent': success_rate,
            'visited_urls_count': len(self.visited_urls),
            'domains_crawled': len(self.domain_url_counts),
            'circuit_breaker_status': 'active' if self.last_backoff_time else 'inactive',
            'error_count': self.error_count,
            'consecutive_errors': self.consecutive_errors
        }
        
        logger.info(f"[Crawler-{self.crawler_id}] Health stats: {json.dumps(health_data)}")
        return health_data

def safe_sqs_receive(queue_url, max_messages=1, visibility_timeout=30, wait_time=20):
    """Safely receive messages from SQS with retries and error handling"""
    retry_count = 0
    max_retries = 3
    
    while retry_count < max_retries:
        try:
            return sqs_client.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=max_messages,
                WaitTimeSeconds=wait_time,
                VisibilityTimeout=visibility_timeout
            )
        except ClientError as e:
            retry_count += 1
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            logger.warning(f"SQS receive error: {error_code} - {str(e)}")
            
            if retry_count < max_retries:
                backoff_time = 2 ** retry_count  # Exponential backoff
                logger.info(f"Retrying SQS receive in {backoff_time} seconds (attempt {retry_count}/{max_retries})")
                time.sleep(backoff_time)
            else:
                logger.error(f"Failed to receive from SQS after {max_retries} attempts")
                raise
        except Exception as e:
            logger.error(f"Unexpected error receiving from SQS: {str(e)}")
            logger.debug(traceback.format_exc())
            raise

def safe_delete_message(queue_url, receipt_handle):
    """Safely delete message from SQS with retries"""
    retry_count = 0
    max_retries = 3
    
    while retry_count < max_retries:
        try:
            return sqs_client.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=receipt_handle
            )
        except ClientError as e:
            retry_count += 1
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            logger.warning(f"SQS delete error: {error_code} - {str(e)}")
            
            if retry_count < max_retries:
                backoff_time = 2 ** retry_count  # Exponential backoff
                logger.info(f"Retrying SQS delete in {backoff_time} seconds (attempt {retry_count}/{max_retries})")
                time.sleep(backoff_time)
            else:
                logger.error(f"Failed to delete from SQS after {max_retries} attempts")
                return False
        except Exception as e:
            logger.error(f"Unexpected error deleting from SQS: {str(e)}")
            logger.debug(traceback.format_exc())
            return False
    
    return True

def crawler_process():
    logger.info("*** Crawler node started - Processing URLs and sending discovered URLs to queue ***")
    logger.info("*** FLOW: 1) Pull URLs from queue → 2) Crawl each URL → 3) Extract links → 4) Send links back to queue ***")
    
    try:
        crawler = Crawler()
        
        # Try to load previous state for recovery (useful after crashes)
        state_files = [f for f in os.listdir('logs') if f.startswith('crawler_state_') and f.endswith('.json')]
        if state_files:
            most_recent = max(state_files, key=lambda f: os.path.getmtime(os.path.join('logs', f)))
            crawler.load_state(os.path.join('logs', most_recent))
        
        # Send a heartbeat message to signify this crawler is active
        try:
            heartbeat = {
                'crawler_id': crawler.crawler_id,
                'hostname': crawler.hostname,
                'timestamp': datetime.now().isoformat(),
                'tag': MessageTags.HEARTBEAT,
                'status': 'starting'
            }
            sqs_client.send_message(
                QueueUrl=SQS_URLS_QUEUE,
                MessageBody=json.dumps(heartbeat)
            )
            logger.info(f"Sent startup heartbeat message with tag {MessageTags.HEARTBEAT}")
        except Exception as e:
            logger.warning(f"Could not send heartbeat: {str(e)}")
        
        # Optional: Test URL extraction on a specific URL
        # crawler.test_extract_and_queue("https://example.com")
        
        health_check_interval = 50  # Report health every 50 loop iterations
        loop_count = 0
        
        while True:
            try:
                # Check loop health periodically
                loop_count += 1
                if loop_count % health_check_interval == 0:
                    health = crawler.health_check()
                    
                    # Send a health heartbeat message
                    try:
                        heartbeat = {
                            'crawler_id': crawler.crawler_id,
                            'hostname': crawler.hostname,
                            'timestamp': datetime.now().isoformat(),
                            'tag': MessageTags.HEARTBEAT,
                            'status': 'running',
                            'health': health
                        }
                        sqs_client.send_message(
                            QueueUrl=SQS_URLS_QUEUE,
                            MessageBody=json.dumps(heartbeat)
                        )
                    except Exception as e:
                        logger.warning(f"Could not send health heartbeat: {str(e)}")
                
                # Log that we're waiting for messages
                logger.info(f"[Crawler-{crawler.crawler_id}] Waiting for URLs from the queue...")
                
                # Receive URL from SQS with robust error handling
                response = safe_sqs_receive(
                    SQS_URLS_QUEUE,
                    max_messages=1,
                    visibility_timeout=60,  # Increased to give more processing time
                    wait_time=20
                )

                if 'Messages' in response:
                    msg_count = len(response['Messages'])
                    logger.info(f"[Crawler-{crawler.crawler_id}] Received {msg_count} message(s) from queue")
                    
                    for message in response['Messages']:
                        receipt_handle = message['ReceiptHandle']
                        message_id = message.get('MessageId', 'unknown')
                        
                        try:
                            body = json.loads(message['Body'])
                            
                            # Extract the tag to identify message type
                            message_tag = body.get('tag', MessageTags.URL_NEW)  # Default to URL_NEW if not specified
                            
                            # Handle different message types
                            if message_tag == MessageTags.HEARTBEAT:
                                # Skip processing heartbeat messages from other crawlers
                                logger.info(f"[Crawler-{crawler.crawler_id}] Skipping heartbeat message from crawler {body.get('crawler_id', 'unknown')}")
                                safe_delete_message(SQS_URLS_QUEUE, receipt_handle)
                                continue
                                
                            elif message_tag == MessageTags.SYSTEM_COMMAND:
                                # Handle system commands
                                command = body.get('command', '')
                                logger.info(f"[Crawler-{crawler.crawler_id}] Received system command: {command}")
                                
                                if command == 'shutdown':
                                    logger.info(f"[Crawler-{crawler.crawler_id}] Shutdown command received, exiting...")
                                    # Save state before exit
                                    crawler.save_state()
                                    safe_delete_message(SQS_URLS_QUEUE, receipt_handle)
                                    # Exit the process
                                    return
                                
                                safe_delete_message(SQS_URLS_QUEUE, receipt_handle)
                                continue
                                
                            elif message_tag == MessageTags.SHUTDOWN:
                                # Handle shutdown signal
                                logger.info(f"[Crawler-{crawler.crawler_id}] Shutdown signal received, exiting...")
                                # Save state before exit
                                crawler.save_state()
                                safe_delete_message(SQS_URLS_QUEUE, receipt_handle)
                                # Exit the process
                                return
                                
                            # Process URL messages (any URL_* tag)
                            elif message_tag in [MessageTags.URL_NEW, MessageTags.URL_RETRY, MessageTags.URL_PRIORITY]:
                                url = body['url']
                                
                                logger.info(f"[Crawler-{crawler.crawler_id}] Received URL from queue: {url} (MessageId: {message_id}, Tag: {message_tag})")
                                
                                # For priority URLs, reduce the crawl delay
                                original_delay = crawler.crawl_delay
                                if message_tag == MessageTags.URL_PRIORITY:
                                    crawler.crawl_delay = crawler.crawl_delay / 2
                                    logger.info(f"[Crawler-{crawler.crawler_id}] Processing priority URL with reduced delay ({crawler.crawl_delay}s)")
                                
                                # Process the URL (fetches content and queues discovered URLs)
                                crawler.process_url(url)
                                
                                # Restore original delay if changed
                                if message_tag == MessageTags.URL_PRIORITY:
                                    crawler.crawl_delay = original_delay
                                
                                # Delete the message from the queue after successful processing
                                if safe_delete_message(SQS_URLS_QUEUE, receipt_handle):
                                    logger.info(f"[Crawler-{crawler.crawler_id}] Successfully processed and deleted message for {url}")
                                else:
                                    logger.warning(f"[Crawler-{crawler.crawler_id}] Message processed but could not be deleted from queue")
                            
                            else:
                                # Unknown message type
                                logger.warning(f"[Crawler-{crawler.crawler_id}] Received message with unknown tag: {message_tag}")
                                safe_delete_message(SQS_URLS_QUEUE, receipt_handle)
                            
                        except json.JSONDecodeError as e:
                            logger.error(f"[Crawler-{crawler.crawler_id}] Error decoding message: {str(e)}")
                            # Send to DLQ and then delete
                            crawler.send_to_dlq(message.get('Body', ''), f"JSON decode error: {str(e)}")
                            safe_delete_message(SQS_URLS_QUEUE, receipt_handle)
                        except KeyError as e:
                            logger.error(f"[Crawler-{crawler.crawler_id}] Missing required field in message: {str(e)}")
                            crawler.send_to_dlq(message.get('Body', ''), f"Missing field: {str(e)}")
                            safe_delete_message(SQS_URLS_QUEUE, receipt_handle)
                        except Exception as e:
                            logger.error(f"[Crawler-{crawler.crawler_id}] Error processing message: {str(e)}")
                            logger.debug(traceback.format_exc())
                            # Return the message to the queue for retry or send to DLQ if max retries exceeded
                            try:
                                # Check if this message has been retried too many times
                                approx_receive_count = int(message.get('Attributes', {}).get('ApproximateReceiveCount', 1))
                                if approx_receive_count >= 3:  # Max retries
                                    logger.warning(f"[Crawler-{crawler.crawler_id}] Message exceeded max retries ({approx_receive_count}), sending to DLQ")
                                    crawler.send_to_dlq(
                                        message.get('Body', ''), 
                                        f"Failed after {approx_receive_count} attempts: {str(e)}"
                                    )
                                    safe_delete_message(SQS_URLS_QUEUE, receipt_handle)
                                else:
                                    # Requeue as a retry message
                                    try:
                                        # Modify the message to indicate it's a retry
                                        retry_body = json.loads(message.get('Body', '{}'))
                                        retry_body['tag'] = MessageTags.URL_RETRY
                                        retry_body['retry_count'] = approx_receive_count
                                        retry_body['last_error'] = str(e)
                                        retry_body['timestamp'] = datetime.now().isoformat()
                                        
                                        # Delete the original message
                                        safe_delete_message(SQS_URLS_QUEUE, receipt_handle)
                                        
                                        # Send as a new message
                                        sqs_client.send_message(
                                            QueueUrl=SQS_URLS_QUEUE,
                                            MessageBody=json.dumps(retry_body)
                                        )
                                        logger.info(f"[Crawler-{crawler.crawler_id}] Requeued as retry message with tag {MessageTags.URL_RETRY}")
                                    except Exception as e2:
                                        # Fall back to visibility timeout if we can't requeue
                                        visibility_timeout = min(30 * approx_receive_count, 720)  # Increase timeout, max 12 minutes
                                        sqs_client.change_message_visibility(
                                            QueueUrl=SQS_URLS_QUEUE,
                                            ReceiptHandle=receipt_handle,
                                            VisibilityTimeout=visibility_timeout
                                        )
                                        logger.info(f"[Crawler-{crawler.crawler_id}] Returned message to queue with visibility timeout of {visibility_timeout}s")
                            except Exception as e2:
                                logger.error(f"[Crawler-{crawler.crawler_id}] Error handling failed message: {str(e2)}")
                else:
                    logger.info(f"[Crawler-{crawler.crawler_id}] No messages received from queue")
                
                # Small delay between queue checks
                time.sleep(2)
                
            except Exception as e:
                logger.error(f"[Crawler-{crawler.crawler_id}] Error in crawler process: {str(e)}")
                logger.error(traceback.format_exc())
                # Save state after error to enable recovery
                crawler.save_state()
                time.sleep(5)  # Wait before retrying
    
    except KeyboardInterrupt:
        logger.info("Crawler shutting down gracefully due to keyboard interrupt")
        crawler.save_state()  # Save state before exiting
    except Exception as e:
        logger.critical(f"Fatal error in crawler process: {str(e)}")
        logger.critical(traceback.format_exc())
        # Attempt one final state save
        try:
            crawler.save_state()
        except:
            pass
        # Exit with error code
        sys.exit(1)

if __name__ == '__main__':
    # Check if we're sending a command
    if len(sys.argv) > 1 and sys.argv[1] == 'command':
        if len(sys.argv) < 3:
            print("Usage: python crawler_node.py command [shutdown|priority-url <url>|status]")
            sys.exit(1)
            
        command = sys.argv[2]
        
        try:
            # Initialize SQS client
            session = boto3.Session(region_name=AWS_REGION)
            sqs_client = session.client('sqs')
            
            if command == 'shutdown':
                # Send shutdown command to all crawlers
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
                
            elif command == 'priority-url' and len(sys.argv) > 3:
                # Send a URL with priority tag
                url = sys.argv[3]
                message = {
                    'tag': MessageTags.URL_PRIORITY,
                    'url': url,
                    'timestamp': datetime.now().isoformat(),
                    'sender': socket.gethostname()
                }
                response = sqs_client.send_message(
                    QueueUrl=SQS_URLS_QUEUE,
                    MessageBody=json.dumps(message)
                )
                print(f"Priority URL sent: {url}. MessageId: {response.get('MessageId')}")
                
            elif command == 'status':
                # Send a command to get status from all crawlers
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
                print("Available commands: shutdown, priority-url <url>, status")
                sys.exit(1)
                
            sys.exit(0)
            
        except Exception as e:
            print(f"Error sending command: {str(e)}")
            traceback.print_exc()
            sys.exit(1)
    
    # Set up signal handlers for graceful shutdown
    try:
        import signal
        def signal_handler(sig, frame):
            logger.info(f"Received signal {sig}, shutting down gracefully")
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
    except Exception as e:
        logger.warning(f"Could not set up signal handlers: {str(e)}")
    
    # Start the crawler process
    crawler_process()    