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
SQS_DLQ_URL = os.getenv('SQS_DLQ_URL', '')  # Dead Letter Queue
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')
MAX_RETRIES = int(os.getenv('MAX_RETRIES', '3'))
HEALTH_CHECK_INTERVAL = int(os.getenv('HEALTH_CHECK_INTERVAL', '300'))  # 5 minutes

logger.info(f"Using SQS Queue URL: {SQS_QUEUE_URL}")
logger.info(f"Using AWS Region: {AWS_REGION}")
logger.info(f"Using S3 Bucket: {S3_BUCKET_NAME}")
if SQS_DLQ_URL:
    logger.info(f"Using Dead Letter Queue: {SQS_DLQ_URL}")

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
                "message_type": body.get('message_type', 'UNKNOWN')
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

    def process_sqs_message(self, message):
        try:
            body = json.loads(message['Body'])
            url = body['url']
            content = body['content']
            
            # Handle message type tag
            msg_type = body.get('message_type', 'CONTENT')
            
            logger.info(f"Processing {msg_type} content from {url}")
            
            # Successful indexing
            if self.index_document(url, content, msg_type):
                # Delete the message from the queue
                sqs_client.delete_message(
                    QueueUrl=SQS_QUEUE_URL,
                    ReceiptHandle=message['ReceiptHandle']
                )
                logger.info(f"Successfully processed and deleted message for {url}")
                return True
            else:
                # Failed to index, send to DLQ
                self.send_to_dlq(message, "Failed to index document")
                
                # Still delete from main queue
                sqs_client.delete_message(
                    QueueUrl=SQS_QUEUE_URL,
                    ReceiptHandle=message['ReceiptHandle']
                )
                return False
            
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding message: {str(e)}")
            logger.error(f"Raw message body: {message.get('Body', 'No body')}")
            self.send_to_dlq(message, f"JSON decode error: {str(e)}")
        except KeyError as e:
            logger.error(f"Missing key in message: {str(e)}")
            logger.error(f"Message body: {message.get('Body', 'No body')}")
            self.send_to_dlq(message, f"Missing key: {str(e)}")
        except Exception as e:
            logger.error(f"Error processing message: {str(e)}")
            self.send_to_dlq(message, f"General error: {str(e)}")
        
        # Always try to delete the message to prevent reprocessing
        try:
            sqs_client.delete_message(
                QueueUrl=SQS_QUEUE_URL,
                ReceiptHandle=message['ReceiptHandle']
            )
        except Exception as e:
            logger.error(f"Error deleting message: {str(e)}")
            return False
        
        return False

    def indexer_loop(self):
        consecutive_empty = 0
        
        while True:
            try:
                # Periodic health check
                if time.time() - self.last_health_check > HEALTH_CHECK_INTERVAL:
                    self.health_check()
                
                # Receive messages from SQS with backoff for empty responses
                wait_time = 20  # Default long polling
                if consecutive_empty > 5:
                    wait_time = min(20, consecutive_empty)  # Increase wait time when queue is empty
                
                response = sqs_client.receive_message(
                    QueueUrl=SQS_QUEUE_URL,
                    MaxNumberOfMessages=10,
                    WaitTimeSeconds=wait_time,
                    VisibilityTimeout=30
                )

                if 'Messages' in response:
                    consecutive_empty = 0
                    for message in response['Messages']:
                        self.process_sqs_message(message)
                else:
                    consecutive_empty += 1
                    if consecutive_empty % 10 == 0:
                        logger.info(f"No messages received for {consecutive_empty} consecutive polls")
                        
                        # During idle time, upload index if there are pending changes
                        if self.stats["documents_indexed"] > 0:
                            self._upload_index_to_s3()
                            
                    # Add some jitter to prevent thundering herd when multiple indexers start
                    time.sleep(random.uniform(1, 3))

            except Exception as e:
                logger.error(f"Error in indexer loop: {str(e)}")
                time.sleep(5)  # Wait before retrying

def indexer_process():
    logger.info("Indexer node started")
    indexer = Indexer()
    
    # Log current stats on startup
    logger.info(f"Initial indexer stats: {indexer.get_stats()}")
    
    # Initial health check
    indexer.health_check()
    
    # Main processing loop
    indexer.indexer_loop()

if __name__ == '__main__':
    indexer_process() 
