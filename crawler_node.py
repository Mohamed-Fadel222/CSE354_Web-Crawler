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

    def process_url(self, url):
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
                # Send extracted URLs to the URLs queue
                for new_url in extracted_urls:
                    try:
                        sqs_client.send_message(
                            QueueUrl=SQS_URLS_QUEUE,
                            MessageBody=json.dumps({'url': new_url})
                        )
                        logger.info(f"Sent new URL to queue: {new_url}")
                    except Exception as e:
                        logger.error(f"Error sending URL to queue: {str(e)}")
                
                # Send content to indexer queue
                message_body = {
                    'url': url,
                    'content': extracted_text[:1000],  # Limit content size
                }
                
                try:
                    sqs_client.send_message(
                        QueueUrl=SQS_QUEUE_URL,
                        MessageBody=json.dumps(message_body)
                    )
                    logger.info(f"Sent content to SQS for {url}")
                except Exception as e:
                    logger.error(f"Error sending to SQS: {str(e)}")

def crawler_process():
    logger.info("Crawler node started")
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
                    try:
                        body = json.loads(message['Body'])
                        url = body['url']
                        
                        # Process the URL
                        crawler.process_url(url)
                        
                        # Delete the message from the queue
                        sqs_client.delete_message(
                            QueueUrl=SQS_URLS_QUEUE,
                            ReceiptHandle=message['ReceiptHandle']
                        )
                        
                    except json.JSONDecodeError as e:
                        logger.error(f"Error decoding message: {str(e)}")
                    except Exception as e:
                        logger.error(f"Error processing message: {str(e)}")
                    
                    # Always try to delete the message
                    try:
                        sqs_client.delete_message(
                            QueueUrl=SQS_URLS_QUEUE,
                            ReceiptHandle=message['ReceiptHandle']
                        )
                    except Exception as e:
                        logger.error(f"Error deleting message: {str(e)}")
            
            # Implement crawl delay
            time.sleep(crawler.crawl_delay)
            
        except Exception as e:
            logger.error(f"Error in crawler process: {str(e)}")
            time.sleep(5)  # Wait before retrying

if __name__ == '__main__':
    crawler_process()    

    # Start the crawler process
