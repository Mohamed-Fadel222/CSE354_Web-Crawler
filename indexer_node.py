import logging
import boto3
import os
from dotenv import load_dotenv
from whoosh.index import create_in, open_dir
from whoosh.fields import Schema, TEXT, ID
from whoosh.qparser import QueryParser
import json
import time
import tempfile
import shutil
from message_tags import *  # Import all message tags

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('Indexer')

# AWS Configuration
AWS_REGION = os.getenv('AWS_REGION', 'eu-north-1')
SQS_QUEUE_URL = os.getenv('SQS_QUEUE_URL')
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')
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
            content=TEXT(stored=True)
        )
        self.temp_dir = tempfile.mkdtemp()
        self.index_dir = os.path.join(self.temp_dir, "index")
        os.makedirs(self.index_dir, exist_ok=True)
        
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
        except Exception as e:
            logger.error(f"Error uploading index to S3: {str(e)}")

    def _download_index_from_s3(self):
        """Download the entire index from S3"""
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
        except Exception as e:
            logger.error(f"Error downloading index from S3: {str(e)}")
            raise

    def index_document(self, url, content):
        if not content or not url:
            logger.warning(f"Skipping empty document for URL: {url}")
            # Send error message
            self._send_error_message(f"Empty document for URL: {url}")
            return

        try:
            self.writer.add_document(url=url, content=content)
            self.writer.commit()
            logger.info(f"Successfully indexed document from {url} ({len(content)} chars)")
            self.writer = self.index.writer()  # Create new writer for next document
            
            # Upload updated index to S3
            self._upload_index_to_s3()
        except Exception as e:
            logger.error(f"Error indexing document from {url}: {str(e)}")
            # Send error message
            self._send_error_message(f"Error indexing document from {url}: {str(e)}")
            try:
                self.writer.cancel()
            except:
                pass
            self.writer = self.index.writer()  # Create new writer after error

    def _send_error_message(self, error_msg):
        """Send error message to SQS with error tag"""
        try:
            sqs_client.send_message(
                QueueUrl=SQS_QUEUE_URL,
                MessageBody=json.dumps({
                    'tag': MSG_TAG_ERROR,
                    'message': error_msg,
                    'timestamp': time.time()
                })
            )
            logger.info(f"Sent error message: {error_msg}")
        except Exception as e:
            logger.error(f"Failed to send error message: {str(e)}")

    def _send_heartbeat(self, status_msg="Indexer running"):
        """Send heartbeat message to SQS"""
        try:
            sqs_client.send_message(
                QueueUrl=SQS_QUEUE_URL,
                MessageBody=json.dumps({
                    'tag': MSG_TAG_HEARTBEAT,
                    'message': status_msg,
                    'timestamp': time.time()
                })
            )
            logger.debug(f"Sent heartbeat: {status_msg}")
        except Exception as e:
            logger.error(f"Failed to send heartbeat: {str(e)}")

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
                return [hit['url'] for hit in results]
        except Exception as e:
            logger.error(f"Error searching for {query_text}: {e}")
            return []

    def process_sqs_message(self, message):
        try:
            body = json.loads(message['Body'])
            tag = body.get('tag', MSG_TAG_CONTENT_SUBMISSION)  # Default to content submission if no tag
            
            if tag == MSG_TAG_CONTENT_SUBMISSION:
                url = body.get('url')
                content = body.get('content')
                
                if not url or not content:
                    logger.warning(f"Skipping message with missing url or content, tag: {tag}")
                else:
                    logger.info(f"Processing content from {url} with tag {tag}")
                    self.index_document(url, content)
            elif tag == MSG_TAG_HEARTBEAT:
                logger.info(f"Received heartbeat message: {body.get('message', 'No message')}")
            elif tag == MSG_TAG_ERROR:
                logger.warning(f"Received error message: {body.get('message', 'No message')}")
            else:
                logger.warning(f"Received message with unknown tag: {tag}")
            
            # Delete the message from the queue
            sqs_client.delete_message(
                QueueUrl=SQS_QUEUE_URL,
                ReceiptHandle=message['ReceiptHandle']
            )
            
            logger.info(f"Successfully processed and deleted message with tag {tag}")
            
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding message: {str(e)}")
            logger.error(f"Raw message body: {message.get('Body', 'No body')}")
        except KeyError as e:
            logger.error(f"Missing key in message: {str(e)}")
            logger.error(f"Message body: {message.get('Body', 'No body')}")
        except Exception as e:
            logger.error(f"Error processing message: {str(e)}")
        
        # Always try to delete the message to prevent reprocessing
        try:
            sqs_client.delete_message(
                QueueUrl=SQS_QUEUE_URL,
                ReceiptHandle=message['ReceiptHandle']
            )
        except Exception as e:
            logger.error(f"Error deleting message: {str(e)}")

    def indexer_loop(self):
        last_heartbeat = 0
        while True:
            try:
                # Send heartbeat every 5 minutes
                current_time = time.time()
                if current_time - last_heartbeat > 300:  # 5 minutes
                    self._send_heartbeat()
                    last_heartbeat = current_time
                
                # Receive messages from SQS
                response = sqs_client.receive_message(
                    QueueUrl=SQS_QUEUE_URL,
                    MaxNumberOfMessages=10,
                    WaitTimeSeconds=20,
                    VisibilityTimeout=30
                )

                if 'Messages' in response:
                    for message in response['Messages']:
                        self.process_sqs_message(message)

            except Exception as e:
                logger.error(f"Error in indexer loop: {str(e)}")
                self._send_error_message(f"Error in indexer loop: {str(e)}")
                time.sleep(5)  # Wait before retrying

def indexer_process():
    logger.info("Indexer node started")
    
    # Send heartbeat message to the queue
    try:
        sqs_client.send_message(
            QueueUrl=SQS_QUEUE_URL,
            MessageBody=json.dumps({
                'tag': MSG_TAG_HEARTBEAT,
                'message': 'Indexer node started',
                'timestamp': time.time()
            })
        )
    except Exception as e:
        logger.error(f"Failed to send heartbeat: {str(e)}")
    
    indexer = Indexer()
    indexer.indexer_loop()

if __name__ == '__main__':
    indexer_process() 
