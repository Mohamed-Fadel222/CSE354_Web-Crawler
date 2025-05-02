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
from datetime import datetime

# ─── Suppress SSL warnings (for testing only) ───────────────────────────
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings('ignore')

# ─── Load environment variables ─────────────────────────────────────────
load_dotenv()
AWS_REGION     = os.getenv('AWS_REGION', 'eu-north-1')
SQS_QUEUE_URL  = os.getenv('SQS_QUEUE_URL')       # for content → indexer
SQS_URLS_QUEUE = os.getenv('SQS_URLS_QUEUE')      # for new URLs → crawler
S3_BUCKET      = os.getenv('S3_BUCKET')           # your S3 bucket name

# ─── Configure logging ──────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('Crawler')

# ─── AWS clients ────────────────────────────────────────────────────────
sqs_client = boto3.client('sqs', region_name=AWS_REGION)
s3_client  = boto3.client('s3', region_name=AWS_REGION)

class Crawler:
    def __init__(self):
        self.headers = {
            'User-Agent': (
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                'AppleWebKit/537.36 (KHTML, like Gecko) '
                'Chrome/91.0.4472.124 Safari/537.36'
            )
        }
        self.crawl_delay = 1  # seconds between requests
        self.visited_urls = set()
        self.crawled_count = 0  # Track how many URLs we've successfully crawled
        self.stats_update_interval = 5  # Update stats every N successful crawls
        self.session = requests.Session()
        self.session.verify = False  # disable SSL verify (testing only)
        self.session.headers.update(self.headers)
        self.session.timeout = 10   # seconds
        
        # Try to load existing crawled count
        self._load_stats_from_s3()

    def _load_stats_from_s3(self):
        """Load existing stats from S3"""
        try:
            response = s3_client.get_object(
                Bucket=S3_BUCKET,
                Key='stats/crawled_count.json'
            )
            stats = json.loads(response['Body'].read().decode('utf-8'))
            self.crawled_count = stats.get('count', 0)
            logger.info(f"Loaded existing crawled count: {self.crawled_count}")
        except Exception as e:
            logger.info(f"No existing stats file found, starting from 0: {e}")
    
    def _update_stats_in_s3(self):
        """Update the statistics file in S3"""
        try:
            stats = {
                'count': self.crawled_count,
                'updated_at': datetime.now().isoformat()
            }
            s3_client.put_object(
                Bucket=S3_BUCKET,
                Key='stats/crawled_count.json',
                Body=json.dumps(stats).encode('utf-8'),
                ContentType='application/json'
            )
            logger.info(f"Updated crawled count stats in S3: {self.crawled_count}")
        except Exception as e:
            logger.error(f"Error updating stats in S3: {e}")

    def fetch_page(self, url: str) -> str | None:
        try:
            logger.info(f"Fetching: {url}")
            resp = self.session.get(url)
            resp.raise_for_status()
            return resp.text
        except requests.RequestException as e:
            logger.error(f"Fetch error for {url}: {e}")
            return None

    def extract_urls(self, html: str, base_url: str) -> list[str]:
        soup = BeautifulSoup(html, 'html.parser')
        found = []
        for a in soup.find_all('a', href=True):
            absolute = urljoin(base_url, a['href'])
            if self.is_valid_url(absolute) and absolute not in self.visited_urls:
                found.append(absolute)
        logger.info(f" → {len(found)} URLs extracted from {base_url}")
        return found[:10]

    def extract_text(self, html: str) -> str:
        soup = BeautifulSoup(html, 'html.parser')
        for tag in soup(['script', 'style', 'nav', 'header', 'footer']):
            tag.decompose()
        text = soup.get_text(separator=' ', strip=True)
        return ' '.join(text.split())

    def is_valid_url(self, url: str) -> bool:
        p = urlparse(url)
        return bool(p.scheme in ('http', 'https') and p.netloc)

    def process_url(self, url: str, receipt_handle: str):
        if url in self.visited_urls:
            logger.info(f"Skipping (visited): {url}")
            # Delete the message to keep it from reappearing in the queue
            try:
                sqs_client.delete_message(
                    QueueUrl=SQS_URLS_QUEUE,
                    ReceiptHandle=receipt_handle
                )
                logger.info(f"Deleted previously visited URL from queue: {url}")
            except Exception as e:
                logger.error(f"Error deleting message for {url}: {e}")
            return
            
        self.visited_urls.add(url)
        
        # Update visibility timeout to keep this URL marked as "In Progress"
        try:
            sqs_client.change_message_visibility(
                QueueUrl=SQS_URLS_QUEUE,
                ReceiptHandle=receipt_handle,
                VisibilityTimeout=60  # Extend visibility timeout while processing
            )
            logger.info(f"Extended visibility timeout for: {url}")
        except Exception as e:
            logger.error(f"Failed to extend visibility timeout: {e}")

        html = self.fetch_page(url)
        if not html:
            # Delete the message even if we couldn't fetch it to avoid retries
            try:
                sqs_client.delete_message(
                    QueueUrl=SQS_URLS_QUEUE,
                    ReceiptHandle=receipt_handle
                )
                logger.info(f"Deleted unfetchable URL from queue: {url}")
            except Exception as e:
                logger.error(f"Error deleting message for {url}: {e}")
            return

        # ─── Store raw HTML in S3 ─────────────────────────────────────
        parsed = urlparse(url)
        key_base = (parsed.netloc + parsed.path).strip('/')
        key_base = key_base.replace('/', '_') or parsed.netloc

        try:
            s3_client.put_object(
                Bucket=S3_BUCKET,
                Key=f'raw/{key_base}.html',
                Body=html.encode('utf-8'),
                ContentType='text/html'
            )
            logger.info(f"Uploaded raw HTML to s3://{S3_BUCKET}/raw/{key_base}.html")
        except Exception as e:
            logger.error(f"S3 upload (raw) failed for {url}: {e}")

        # ─── Extract & store cleaned text in S3 ────────────────────────
        text = self.extract_text(html)
        if text:
            try:
                s3_client.put_object(
                    Bucket=S3_BUCKET,
                    Key=f'text/{key_base}.txt',
                    Body=text.encode('utf-8'),
                    ContentType='text/plain'
                )
                logger.info(f"Uploaded text to s3://{S3_BUCKET}/text/{key_base}.txt")
                
                # Increment crawled count
                self.crawled_count += 1
                
                # Update stats periodically
                if self.crawled_count % self.stats_update_interval == 0:
                    self._update_stats_in_s3()
                    
            except Exception as e:
                logger.error(f"S3 upload (text) failed for {url}: {e}")

        # ─── Send newly discovered URLs back into the crawl queue ───────
        for new_url in self.extract_urls(html, url):
            try:
                sqs_client.send_message(
                    QueueUrl=SQS_URLS_QUEUE,
                    MessageBody=json.dumps({'url': new_url})
                )
                logger.info(f"Enqueued new URL: {new_url}")
            except Exception as e:
                logger.error(f"Error enqueuing URL {new_url}: {e}")

        # ─── Send this page's content snippet to the indexer queue ─────
        snippet = text[:1000]
        try:
            sqs_client.send_message(
                QueueUrl=SQS_QUEUE_URL,
                MessageBody=json.dumps({'url': url, 'content': snippet})
            )
            logger.info(f"Sent content snippet for indexing: {url}")
        except Exception as e:
            logger.error(f"Error sending content for {url}: {e}")
            
        # ─── Delete the message after successful processing ───────────
        try:
            sqs_client.delete_message(
                QueueUrl=SQS_URLS_QUEUE,
                ReceiptHandle=receipt_handle
            )
            logger.info(f"Successfully processed and deleted message for: {url}")
        except Exception as e:
            logger.error(f"Error deleting message for {url}: {e}")

def crawler_process():
    logger.info("Crawler started")
    crawler = Crawler()

    while True:
        try:
            # Lower VisibilityTimeout to 30 seconds so URLs show as "In Progress"
            # for a reasonable time in the dashboard
            resp = sqs_client.receive_message(
                QueueUrl=SQS_URLS_QUEUE,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=10,
                VisibilityTimeout=30
            )
            
            messages = resp.get('Messages', [])
            
            if not messages:
                logger.info("No messages in queue, waiting...")
                time.sleep(5)
                continue
                
            for msg in messages:
                try:
                    body = json.loads(msg['Body'])
                    url = body.get('url')
                    if url:
                        logger.info(f"Processing URL: {url}")
                        crawler.process_url(url, msg['ReceiptHandle'])
                    else:
                        logger.error(f"Invalid message format, no URL found: {body}")
                        # Delete malformed message
                        sqs_client.delete_message(
                            QueueUrl=SQS_URLS_QUEUE,
                            ReceiptHandle=msg['ReceiptHandle']
                        )
                except json.JSONDecodeError as e:
                    logger.error(f"Invalid JSON in message: {e}")
                    # Delete invalid message
                    sqs_client.delete_message(
                        QueueUrl=SQS_URLS_QUEUE,
                        ReceiptHandle=msg['ReceiptHandle']
                    )
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    
            # Wait between polling to avoid excessive API calls
            time.sleep(crawler.crawl_delay)
            
        except Exception as e:
            logger.error(f"Error in crawler loop: {e}")
            time.sleep(5)
        
        # Update stats before exit
        except KeyboardInterrupt:
            logger.info("Crawler shutting down, updating stats...")
            crawler._update_stats_in_s3()
            break

if __name__ == '__main__':
    crawler_process()
