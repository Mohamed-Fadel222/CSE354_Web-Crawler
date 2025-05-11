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
            base_domain = urlparse(base_url).netloc
            
            for link in soup.find_all('a', href=True):
                href = link['href']
                absolute_url = urljoin(base_url, href)
                parsed_url = urlparse(absolute_url)
                
                # Only follow URLs from the same domain
                if (parsed_url.netloc == base_domain and 
                    self.is_valid_url(absolute_url) and 
                    absolute_url not in self.visited_urls):
                    urls.append(absolute_url)
            
            # Limit to 10 URLs per page as requested
            urls = urls[:10]
            logger.info(f"Extracted {len(urls)} internal URLs from {base_url} (limited to 10 max)")
            return urls
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

    def get_domain_from_url(self, url):
        try:
            return urlparse(url).netloc
        except:
            return None

    def process_url(self, url):
        """
        Process a URL by crawling it and sending discovered URLs to the queue
        """
        # Skip if already visited
        if url in self.visited_urls:
            logger.info(f"Already visited {url}, skipping")
            return

        logger.info(f"Processing URL: {url}")
        self.visited_urls.add(url)
        
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
                }
                
                try:
                    sqs_client.send_message(
                        QueueUrl=SQS_QUEUE_URL,
                        MessageBody=json.dumps(message_body)
                    )
                    logger.info(f"Sent content to SQS for {url}")
                except Exception as e:
                    logger.error(f"Error sending to SQS: {str(e)}")
            
            # Queue all discovered URLs to the URLs queue (letting master node handle them)
            if extracted_urls:
                urls_queued = 0
                for new_url in extracted_urls:
                    try:
                        sqs_client.send_message(
                            QueueUrl=SQS_URLS_QUEUE,
                            MessageBody=json.dumps({'url': new_url})
                        )
                        urls_queued += 1
                        logger.info(f"Queued new URL for master node: {new_url}")
                    except Exception as e:
                        logger.error(f"Error queuing URL {new_url}: {str(e)}")
                
                logger.info(f"Queued {urls_queued} URLs from {url} for further processing")
                            
        # Implement crawl delay
        time.sleep(self.crawl_delay)

def crawler_process():
    logger.info("Crawler node started - Processing URLs and sending discovered URLs to queue")
    crawler = Crawler()
    
    while True:
        try:
            # Receive URL from SQS
            response = sqs_client.receive_message(
                QueueUrl=SQS_URLS_QUEUE,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=20,
                VisibilityTimeout=30
            )

            if 'Messages' in response:
                for message in response['Messages']:
                    receipt_handle = message['ReceiptHandle']
                    try:
                        body = json.loads(message['Body'])
                        url = body['url']
                        
                        # Process the URL (fetches content and queues discovered URLs)
                        crawler.process_url(url)
                        
                        # Delete the message from the queue after successful processing
                        sqs_client.delete_message(
                            QueueUrl=SQS_URLS_QUEUE,
                            ReceiptHandle=receipt_handle
                        )
                        logger.info(f"Successfully processed and deleted message for {url}")
                        
                    except json.JSONDecodeError as e:
                        logger.error(f"Error decoding message: {str(e)}")
                        # Delete malformed messages
                        sqs_client.delete_message(
                            QueueUrl=SQS_URLS_QUEUE,
                            ReceiptHandle=receipt_handle
                        )
                    except Exception as e:
                        logger.error(f"Error processing message: {str(e)}")
                        # Return the message to the queue for retry
                        try:
                            sqs_client.change_message_visibility(
                                QueueUrl=SQS_URLS_QUEUE,
                                ReceiptHandle=receipt_handle,
                                VisibilityTimeout=0  # Make immediately visible for retry
                            )
                            logger.info(f"Returned message to queue for retry")
                        except Exception as e:
                            logger.error(f"Error changing message visibility: {str(e)}")
            
            # Small delay between queue checks
            time.sleep(2)
            
        except Exception as e:
            logger.error(f"Error in crawler process: {str(e)}")
            time.sleep(5)  # Wait before retrying

if __name__ == '__main__':
    crawler_process()    