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
import hashlib
import datetime

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

# Message Tags
MSG_TAG_INFO = 0       # Regular informational messages
MSG_TAG_URL = 1        # URL processing messages
MSG_TAG_CONTENT = 2    # Content processing messages  
MSG_TAG_WARNING = 99   # Warning messages
MSG_TAG_ERROR = 999    # Error messages

class Indexer:
    def __init__(self):
        self.schema = Schema(
            url=ID(stored=True),
            content=TEXT(stored=True)
        )
        self.temp_dir = tempfile.mkdtemp()
        self.index_dir = os.path.join(self.temp_dir, "index")
        os.makedirs(self.index_dir, exist_ok=True)
        
        # Ensure required S3 directories exist
        self._ensure_s3_directories()
        
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
        
        # Load master index file or create new one
        self.master_index = self._load_master_index()

    def _ensure_s3_directories(self):
        """Check if required directories exist in S3 and create them if not"""
        try:
            # Check if searchable_index/documents/ exists
            response = s3_client.list_objects_v2(
                Bucket=S3_BUCKET_NAME,
                Prefix='searchable_index/documents/',
                MaxKeys=1
            )
            
            # If 'Contents' is not in response, the directory doesn't exist
            if 'Contents' not in response:
                logger.info("searchable_index/documents/ directory not found, creating it...")
                # Create empty placeholder file to ensure directory exists
                s3_client.put_object(
                    Bucket=S3_BUCKET_NAME,
                    Key='searchable_index/documents/.placeholder',
                    Body=''
                )
                logger.info("Created searchable_index/documents/ directory")
                
            # Also ensure searchable_index/ exists (for master_index.json)
            response = s3_client.list_objects_v2(
                Bucket=S3_BUCKET_NAME,
                Prefix='searchable_index/',
                MaxKeys=1
            )
            
            if 'Contents' not in response:
                logger.info("searchable_index/ directory not found, creating it...")
                s3_client.put_object(
                    Bucket=S3_BUCKET_NAME,
                    Key='searchable_index/.placeholder',
                    Body=''
                )
                logger.info("Created searchable_index/ directory")
                
            logger.info("S3 directory structure verified")
        except Exception as e:
            logger.error(f"Error ensuring S3 directories exist: {str(e)}")

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

    def _load_master_index(self):
        """Load or create the master index file that maps keywords to document IDs"""
        try:
            response = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key='searchable_index/master_index.json')
            master_index = json.loads(response['Body'].read().decode('utf-8'))
            logger.info(f"Loaded master index with {len(master_index['documents'])} documents")
            return master_index
        except Exception as e:
            logger.info(f"Creating new master index: {str(e)}")
            # Create a new master index
            new_index = {
                "last_updated": datetime.datetime.now().isoformat(),
                "document_count": 0,
                "documents": {},
                "keywords": {}
            }
            
            # Save the new master index to S3
            try:
                s3_client.put_object(
                    Bucket=S3_BUCKET_NAME,
                    Key='searchable_index/master_index.json',
                    Body=json.dumps(new_index),
                    ContentType='application/json'
                )
                logger.info("Created and saved new master index to S3")
            except Exception as e:
                logger.error(f"Error saving new master index to S3: {str(e)}")
                
            return new_index

    def _save_master_index(self):
        """Save the master index file to S3"""
        try:
            # Update timestamp
            self.master_index["last_updated"] = datetime.datetime.now().isoformat()
            
            # Save to S3
            s3_client.put_object(
                Bucket=S3_BUCKET_NAME,
                Key='searchable_index/master_index.json',
                Body=json.dumps(self.master_index),
                ContentType='application/json'
            )
            logger.info("Successfully saved master index to S3")
        except Exception as e:
            logger.error(f"Error saving master index to S3: {str(e)}")

    def _extract_keywords(self, content):
        """Extract important keywords from the content for faster searching"""
        # Simple keyword extraction - split by spaces and filter common words
        words = content.lower().split()
        # Filter out common words and short words
        stopwords = {'the', 'a', 'an', 'in', 'on', 'at', 'to', 'for', 'with', 'by', 'of', 'and', 'or', 'is', 'are'}
        keywords = [word for word in words if word not in stopwords and len(word) > 3]
        # Return unique keywords
        return list(set(keywords))

    def index_document(self, url, content):
        if not content or not url:
            logger.warning(f"Skipping empty document for URL: {url}")
            return

        try:
            # 1. Add to Whoosh index for full-text search
            self.writer.add_document(url=url, content=content)
            self.writer.commit()
            logger.info(f"Successfully indexed document in Whoosh: {url} ({len(content)} chars)")
            self.writer = self.index.writer()  # Create new writer for next document
            
            # 2. Create searchable JSON file for this document
            doc_id = hashlib.md5(url.encode()).hexdigest()
            document_data = {
                "url": url,
                "content": content[:5000],  # Store more content (up to 5000 chars)
                "content_length": len(content),
                "timestamp": datetime.datetime.now().isoformat(),
                "id": doc_id,
                "tag": MSG_TAG_CONTENT  # Add tag for content
            }
            
            # Extract keywords for faster searching
            keywords = self._extract_keywords(content)
            document_data["keywords"] = keywords
            
            # Save individual document JSON
            s3_client.put_object(
                Bucket=S3_BUCKET_NAME,
                Key=f'searchable_index/documents/{doc_id}.json',
                Body=json.dumps(document_data),
                ContentType='application/json'
            )
            
            # 3. Update master index
            self.master_index["documents"][doc_id] = {
                "url": url,
                "timestamp": document_data["timestamp"],
                "tag": MSG_TAG_CONTENT
            }
            
            # Add keyword mappings for faster searching
            for keyword in keywords:
                if keyword not in self.master_index["keywords"]:
                    self.master_index["keywords"][keyword] = []
                if doc_id not in self.master_index["keywords"][keyword]:
                    self.master_index["keywords"][keyword].append(doc_id)
            
            # Update document count
            self.master_index["document_count"] = len(self.master_index["documents"])
            
            # 4. Save master index
            self._save_master_index()
            
            # 5. Upload full index to S3
            self._upload_index_to_s3()
            
            logger.info(f"Successfully indexed document with optimized search: {url}")
        except Exception as e:
            logger.error(f"Error indexing document from {url}: {str(e)}")
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
            # 1. Fast search using master index
            results = []
            query_words = query_text.lower().split()
            
            # Check if any keywords match
            matching_docs = set()
            for word in query_words:
                if len(word) > 3 and word in self.master_index["keywords"]:
                    matching_docs.update(self.master_index["keywords"][word])
            
            # Get the document URLs
            for doc_id in matching_docs:
                if doc_id in self.master_index["documents"]:
                    # Verify document actually exists in S3 before adding to results
                    try:
                        doc_key = f'searchable_index/documents/{doc_id}.json'
                        s3_client.head_object(Bucket=S3_BUCKET_NAME, Key=doc_key)
                        # Document exists, add to results
                        results.append(self.master_index["documents"][doc_id]["url"])
                    except Exception as e:
                        logger.warning(f"Document {doc_id} referenced in master index but not found in S3: {str(e)}")
                        continue
            
            logger.info(f"Quick search found {len(results)} results for '{query_text}'")
            
            # 2. If not enough results, use the more thorough Whoosh search
            if len(results) < 5:
                with self.index.searcher() as searcher:
                    query = QueryParser("content", self.index.schema).parse(query_text)
                    whoosh_results = searcher.search(query, limit=10)
                    whoosh_urls = [hit['url'] for hit in whoosh_results]
                    
                    # Add any new results
                    for url in whoosh_urls:
                        if url not in results:
                            results.append(url)
                    
                logger.info(f"Combined search found {len(results)} results for '{query_text}'")
            
            return results
        except Exception as e:
            logger.error(f"Error searching for {query_text}: {e}")
            return []

    def process_sqs_message(self, message):
        try:
            body = json.loads(message['Body'])
            tag = body.get('tag', MSG_TAG_CONTENT)  # Default to content tag if not specified
            
            # Handle different message types based on tag
            if tag == MSG_TAG_CONTENT:
                url = body.get('url')
                content = body.get('content')
                
                if not url:
                    logger.warning(f"Content message missing URL field: {body}")
                    return
                    
                if content:
                    logger.info(f"Processing content from {url} (tag: {tag})")
                    self.index_document(url, content)
                else:
                    logger.warning(f"Content message missing content field: {url}")
            elif tag == MSG_TAG_INFO:
                # Handle info/status messages without URL field
                message_type = body.get('message_type')
                if message_type == 'system_status':
                    # These are system status updates from master node - just log them
                    logger.debug(f"Received system status update: {body}")
                else:
                    logger.info(f"Received info message: {body}")
            elif tag == MSG_TAG_ERROR:
                # Log error messages but don't index them
                error = body.get('error', 'Unknown error')
                url = body.get('url', 'No URL specified')
                logger.error(f"Received error message: {error} for URL: {url}")
            elif tag == MSG_TAG_WARNING:
                # Log warning messages
                warning = body.get('message', 'Unknown warning')
                url = body.get('url', 'No URL specified')
                logger.warning(f"Received warning message: {warning} for URL: {url}")
            else:
                logger.warning(f"Received message with unknown tag {tag}: {body}")
            
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
        while True:
            try:
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
                time.sleep(5)  # Wait before retrying

def indexer_process():
    logger.info("Indexer node started")
    indexer = Indexer()
    indexer.indexer_loop()

if __name__ == '__main__':
    indexer_process() 
