import logging
import boto3
import os
from dotenv import load_dotenv
from whoosh.index import create_in, open_dir
from whoosh.fields import Schema, TEXT, ID, DATETIME
from whoosh.qparser import QueryParser
import json
import time
import tempfile
import shutil
from datetime import datetime
import random

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('Indexer')

# AWS Configuration
AWS_REGION = os.getenv('AWS_REGION', 'eu-north-1')
SQS_QUEUE_URL = os.getenv('SQS_QUEUE_URL')
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')
MAX_RETRIES = int(os.getenv('MAX_RETRIES', '3'))
HEALTH_CHECK_INTERVAL = int(os.getenv('HEALTH_CHECK_INTERVAL', '300'))  # 5 minutes

logger.info(f"Using SQS Queue URL: {SQS_QUEUE_URL}")
logger.info(f"Using AWS Region: {AWS_REGION}")
logger.info(f"Using S3 Bucket: {S3_BUCKET_NAME}")

# Initialize AWS clients
sqs_client = boto3.client('sqs', region_name=AWS_REGION)
s3_client = boto3.client('s3', region_name=AWS_REGION)

class Indexer:
    def __init__(self):
        self.schema = Schema(
            url=ID(stored=True),
            content=TEXT(stored=True),
            last_indexed=DATETIME(stored=True),
            msg_type=ID(stored=True)  # Added message type field for tagging
        )
        self.temp_dir = tempfile.mkdtemp()
        self.index_dir = os.path.join(self.temp_dir, "index")
        os.makedirs(self.index_dir, exist_ok=True)
        self.last_health_check = time.time()
        self.retry_backoff = [2, 5, 10, 30, 60]  # Exponential backoff times in seconds
        self.stats = {
            "documents_indexed": 0,
            "failed_documents": 0,
            "retried_documents": 0,
            "message_retries": 0,
            "start_time": datetime.now()
        }
        
        # Try to download existing index from S3
        try:
            self._download_index_from_s3()
            self.index = open_dir(self.index_dir)
            logger.info("Successfully loaded existing index from S3")
        except Exception as e:
            logger.info(f"No existing index found in S3 or error loading: {str(e)}")
            self.index = create_in(self.index_dir, self.schema)
            logger.info("Created new index")
        
        self.writer = self.index.writer()
        logger.info("Indexer initialized")

    def _upload_index_to_s3(self):
        """Upload the entire index directory to S3"""
        try:
            for root, dirs, files in os.walk(self.index_dir):
                for file in files:
                    local_path = os.path.join(root, file)
                    s3_path = os.path.relpath(local_path, self.temp_dir)
                    s3_client.upload_file(local_path, S3_BUCKET_NAME, s3_path)
            logger.info("Successfully uploaded index to S3")
            return True
        except Exception as e:
            logger.error(f"Error uploading index to S3: {str(e)}")
            return False

    def _download_index_from_s3(self):
        """Download the entire index from S3"""
        retry_count = 0
        while retry_count < MAX_RETRIES:
            try:
                # List all objects in the index directory
                paginator = s3_client.get_paginator('list_objects_v2')
                for page in paginator.paginate(Bucket=S3_BUCKET_NAME, Prefix='index/'):
                    if 'Contents' in page:
                        for obj in page['Contents']:
                            s3_path = obj['Key']
                            local_path = os.path.join(self.temp_dir, s3_path)
                            os.makedirs(os.path.dirname(local_path), exist_ok=True)
                            s3_client.download_file(S3_BUCKET_NAME, s3_path, local_path)
                logger.info("Successfully downloaded index from S3")
                return
            except Exception as e:
                retry_count += 1
                if retry_count >= MAX_RETRIES:
                    logger.error(f"Failed to download index after {MAX_RETRIES} attempts: {str(e)}")
                    raise
                backoff_time = self.retry_backoff[min(retry_count-1, len(self.retry_backoff)-1)]
                logger.warning(f"Error downloading index, retry {retry_count}/{MAX_RETRIES} in {backoff_time}s: {str(e)}")
                time.sleep(backoff_time)

    def index_document(self, url, content, msg_type="CONTENT"):
        if not content or not url:
            logger.warning(f"Skipping empty document for URL: {url}")
            return False

        retry_count = 0
        while retry_count < MAX_RETRIES:
            try:
                # Add document with timestamp and message type
                self.writer.add_document(
                    url=url, 
                    content=content, 
                    last_indexed=datetime.now(),
                    msg_type=msg_type
                )
                self.writer.commit()
                logger.info(f"Successfully indexed document from {url} ({len(content)} chars)")
                self.writer = self.index.writer()  # Create new writer for next document
                
                # Upload updated index to S3
                self._upload_index_to_s3()
                self.stats["documents_indexed"] += 1
                return True
            except Exception as e:
                retry_count += 1
                self.stats["retried_documents"] += 1
                if retry_count >= MAX_RETRIES:
                    logger.error(f"Failed to index document after {MAX_RETRIES} attempts: {str(e)}")
                    self.stats["failed_documents"] += 1
                    try:
                        self.writer.cancel()
                    except:
                        pass
                    self.writer = self.index.writer()  # Create new writer after error
                    return False
                
                backoff_time = self.retry_backoff[min(retry_count-1, len(self.retry_backoff)-1)]
                logger.warning(f"Error indexing document, retry {retry_count}/{MAX_RETRIES} in {backoff_time}s: {str(e)}")
                time.sleep(backoff_time)
                try:
                    self.writer.cancel()
                except:
                    pass
                self.writer = self.index.writer()  # Create new writer after error

    def __del__(self):
        """Cleanup temporary directory when the indexer is destroyed"""
        try:
            shutil.rmtree(self.temp_dir)
        except Exception as e:
            logger.error(f"Error cleaning up temporary directory: {str(e)}")

    def search(self, query_text):
        try:
            with self.index.searcher() as searcher:
                query = QueryParser("content", self.index.schema).parse(query_text)
                results = searcher.search(query, limit=10)
                return [{
                    'url': hit['url'],
                    'last_indexed': hit['last_indexed'].strftime('%Y-%m-%d %H:%M:%S') if 'last_indexed' in hit else 'Unknown',
                    'snippet': hit.highlights('content', top=1) if hasattr(hit, 'highlights') else '',
                    'message_type': hit.get('msg_type', 'CONTENT')
                } for hit in results]
        except Exception as e:
            logger.error(f"Error searching for {query_text}: {e}")
            return []

    def get_stats(self):
        """Return indexer statistics"""
        uptime = (datetime.now() - self.stats["start_time"]).total_seconds()
        docs_per_minute = 0
        if uptime > 0:
            docs_per_minute = (self.stats["documents_indexed"] / (uptime / 60))
            
        return {
            "documents_indexed": self.stats["documents_indexed"],
            "failed_documents": self.stats["failed_documents"],
            "retried_documents": self.stats["retried_documents"],
            "message_retries": self.stats["message_retries"],
            "uptime_seconds": int(uptime),
            "indexing_rate": round(docs_per_minute, 2),
            "last_health_check": time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(self.last_health_check))
        }

    def health_check(self):
        """Perform a health check"""
        self.last_health_check = time.time()
        
        # Check if we can read from the index
        try:
            with self.index.searcher() as searcher:
                doc_count = searcher.doc_count()
                logger.info(f"Health check: Index contains {doc_count} documents")
                
            # Check S3 connection
            s3_client.head_bucket(Bucket=S3_BUCKET_NAME)
            
            # Check SQS connection
            sqs_client.get_queue_attributes(
                QueueUrl=SQS_QUEUE_URL,
                AttributeNames=['ApproximateNumberOfMessages']
            )
            
            # Monitor queue health
            self.monitor_queue_health()
            
            logger.info("Health check: All services operational")
            return True
        except Exception as e:
            logger.error(f"Health check failed: {str(e)}")
            return False

    def monitor_queue_health(self):
        """Monitor queue for signs of processing issues"""
        try:
            # Check content queue
            content_response = sqs_client.get_queue_attributes(
                QueueUrl=SQS_QUEUE_URL,
                AttributeNames=['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible']
            )
            
            # Calculate health ratio
            content_visible = int(content_response['Attributes']['ApproximateNumberOfMessages'])
            content_in_flight = int(content_response['Attributes']['ApproximateNumberOfMessagesNotVisible'])
            
            # Check for high in-flight ratio which might indicate stuck processing
            if content_visible + content_in_flight > 0:
                content_health_ratio = content_in_flight / (content_visible + content_in_flight)
                if content_health_ratio > 0.8:  # More than 80% of messages are in-flight
                    logger.warning(f"Content queue health warning: {content_in_flight} in-flight vs {content_visible} visible messages")
            
            return True
        except Exception as e:
            logger.error(f"Error monitoring queue health: {str(e)}")
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
                delay_seconds = min(body['error_count'] * 60, 900)  # Backoff with delay, max 15 minutes
                
                sqs_client.send_message(
                    QueueUrl=SQS_QUEUE_URL,
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

    def process_sqs_message(self, message):
        try:
            # Parse the message body
            body = json.loads(message['Body'])
            
            # Check if this is a retry and log accordingly
            if body.get('error_count', 0) > 0:
                logger.info(f"Processing retry {body['error_count']}/{MAX_RETRIES} for {body.get('url', 'unknown')}")
            
            # Extract content and URL
            url = body.get('url')
            content = body.get('content')
            title = body.get('title', '')
            
            if url and content:
                # Add title to content for better searchability
                if title:
                    full_content = f"{title}\n\n{content}"
                else:
                    full_content = content
                    
                # Index the document
                msg_type = body.get('message_type', 'CONTENT')
                if self.index_document(url, full_content, msg_type):
                    logger.info(f"Successfully processed message for {url}")
                    return True
                else:
                    logger.error(f"Failed to index document for {url}")
                    return False
            else:
                logger.warning(f"Message missing URL or content: {body}")
                return False
                
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding message: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Error processing message: {str(e)}")
            return False

    def indexer_loop(self):
        """Main indexer loop"""
        logger.info("Starting indexer loop")
        
        while True:
            try:
                # Poll for messages from the SQS queue with long polling
                response = sqs_client.receive_message(
                    QueueUrl=SQS_QUEUE_URL,
                    AttributeNames=['All'],
                    MaxNumberOfMessages=1,
                    WaitTimeSeconds=20
                )
                
                if 'Messages' in response:
                    message = response['Messages'][0]
                    receipt_handle = message['ReceiptHandle']
                    
                    try:
                        success = self.process_sqs_message(message)
                        if success:
                            # Delete message from queue only on success
                            sqs_client.delete_message(
                                QueueUrl=SQS_QUEUE_URL,
                                ReceiptHandle=receipt_handle
                            )
                        else:
                            # Handle the failure case
                            self.handle_failed_message(message, "Failed to process message")
                            
                    except Exception as e:
                        logger.error(f"Error processing message: {str(e)}")
                        # Handle the exception
                        self.handle_failed_message(message, str(e))
                else:
                    logger.info("No messages in queue, waiting...")
                    time.sleep(5)
                    
                # Perform health check periodically
                if time.time() - self.last_health_check > HEALTH_CHECK_INTERVAL:
                    self.health_check()
                
            except Exception as e:
                logger.error(f"Error in indexer loop: {str(e)}")
                time.sleep(5)

def indexer_process():
    logger.info("Indexer node started")
    indexer = Indexer()
    
    # Initial health check
    indexer.health_check()
    
    # Start the main indexer loop
    indexer.indexer_loop()

if __name__ == "__main__":
    indexer_process() 
