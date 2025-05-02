import time
import logging
import boto3
import os
from dotenv import load_dotenv
import json
import tempfile
import shutil
from whoosh.index import create_in, exists_in, open_dir
from whoosh.fields import Schema, TEXT, ID
from whoosh.qparser import QueryParser
import io
import tarfile

# ─── Load environment variables ─────────────────────────────────────────
load_dotenv()
AWS_REGION    = os.getenv('AWS_REGION', 'eu-north-1')
SQS_QUEUE_URL = os.getenv('SQS_QUEUE_URL')  # Content queue from crawler
S3_BUCKET     = os.getenv('S3_BUCKET')      # S3 bucket for index storage
INDEX_KEY     = 'search_index/whoosh_index.tar.gz'  # S3 key for the index

# ─── Configure logging ──────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('Indexer')

# ─── AWS clients ────────────────────────────────────────────────────────
sqs_client = boto3.client('sqs', region_name=AWS_REGION)
s3_client = boto3.client('s3', region_name=AWS_REGION)

class Indexer:
    def __init__(self):
        self.index_dir = tempfile.mkdtemp()
        self.schema = Schema(
            url=ID(stored=True, unique=True),
            content=TEXT(stored=True)
        )
        self.index_counter = 0
        self.index_threshold = 10  # Upload index after processing this many items
        self.initialize_index()

    def initialize_index(self):
        """Initialize the index directory or download from S3 if available"""
        try:
            # Try to download existing index from S3
            logger.info(f"Checking for existing index in S3: {S3_BUCKET}/{INDEX_KEY}")
            s3_client.download_file(S3_BUCKET, INDEX_KEY, 'index.tar.gz')
            
            # Extract the tar.gz file
            with tarfile.open('index.tar.gz', 'r:gz') as tar:
                tar.extractall(path=self.index_dir)
            logger.info(f"Downloaded and extracted existing index from S3")
            
            # Clean up
            os.remove('index.tar.gz')
            
        except Exception as e:
            logger.info(f"No existing index found or error downloading: {e}")
            logger.info(f"Creating new index in {self.index_dir}")
            if not exists_in(self.index_dir):
                os.makedirs(self.index_dir, exist_ok=True)
                create_in(self.index_dir, self.schema)
    
    def upload_index_to_s3(self):
        """Compress and upload the index to S3"""
        try:
            logger.info(f"Compressing index for upload to S3")
            
            # Create a tar.gz file in memory
            tar_buffer = io.BytesIO()
            with tarfile.open(fileobj=tar_buffer, mode='w:gz') as tar:
                tar.add(self.index_dir, arcname='')
            
            # Reset buffer position
            tar_buffer.seek(0)
            
            # Upload to S3
            logger.info(f"Uploading index to S3: {S3_BUCKET}/{INDEX_KEY}")
            s3_client.upload_fileobj(tar_buffer, S3_BUCKET, INDEX_KEY)
            logger.info(f"Successfully uploaded index to S3")
            
            # Reset counter
            self.index_counter = 0
            
        except Exception as e:
            logger.error(f"Error uploading index to S3: {e}")
    
    def add_document(self, url, content):
        """Add a document to the index"""
        try:
            idx = open_dir(self.index_dir)
            writer = idx.writer()
            
            # Update document if it exists, otherwise add it
            writer.update_document(url=url, content=content)
            writer.commit()
            
            logger.info(f"Indexed document: {url}")
            self.index_counter += 1
            
            # Upload index to S3 after reaching threshold
            if self.index_counter >= self.index_threshold:
                self.upload_index_to_s3()
                
        except Exception as e:
            logger.error(f"Error indexing document {url}: {e}")
    
    def search(self, query_text, limit=20):
        """Search the index (mainly for testing)"""
        results = []
        try:
            idx = open_dir(self.index_dir)
            with idx.searcher() as searcher:
                query = QueryParser("content", idx.schema).parse(query_text)
                search_results = searcher.search(query, limit=limit)
                for hit in search_results:
                    results.append({
                        'url': hit['url'],
                        'score': hit.score
                    })
        except Exception as e:
            logger.error(f"Search error: {e}")
        return results
    
    def cleanup(self):
        """Remove temporary directory"""
        try:
            shutil.rmtree(self.index_dir)
            logger.info(f"Cleaned up temporary directory: {self.index_dir}")
        except Exception as e:
            logger.error(f"Error cleaning up: {e}")

def indexer_process():
    logger.info("Indexer started")
    indexer = Indexer()

    try:
        while True:
            # Poll SQS for messages
            response = sqs_client.receive_message(
                QueueUrl=SQS_QUEUE_URL,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=20,
                VisibilityTimeout=30
            )
            
            messages = response.get('Messages', [])
            if not messages:
                logger.info("No messages received, continuing...")
                continue
                
            logger.info(f"Received {len(messages)} messages")
            
            # Process messages
            for message in messages:
                try:
                    body = json.loads(message['Body'])
                    url = body.get('url')
                    content = body.get('content')
                    
                    if url and content:
                        indexer.add_document(url, content)
                    
                    # Delete message from queue
                    sqs_client.delete_message(
                        QueueUrl=SQS_QUEUE_URL,
                        ReceiptHandle=message['ReceiptHandle']
                    )
                    
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
            
            # Small delay to prevent CPU overuse
            time.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("Shutting down indexer")
        # Force upload index before exit
        indexer.upload_index_to_s3()
        indexer.cleanup()
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        indexer.upload_index_to_s3()
        indexer.cleanup()

if __name__ == '__main__':
    indexer_process() 
