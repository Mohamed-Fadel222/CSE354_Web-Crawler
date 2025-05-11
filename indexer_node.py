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
import traceback
import sys
import socket
from botocore.exceptions import ClientError

# Message type tag constants for consistent tagging - matches crawler_node.py
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

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/indexer_{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger('Indexer')

# AWS Configuration
AWS_REGION = os.getenv('AWS_REGION', 'eu-north-1')
SQS_QUEUE_URL = os.getenv('SQS_QUEUE_URL')
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')
SQS_DLQ_URL = os.getenv('SQS_DLQ_URL')  # Dead Letter Queue for failed messages

logger.info(f"Using SQS Queue URL: {SQS_QUEUE_URL}")
logger.info(f"Using AWS Region: {AWS_REGION}")
logger.info(f"Using S3 Bucket: {S3_BUCKET_NAME}")
logger.info(f"Dead Letter Queue: {SQS_DLQ_URL if SQS_DLQ_URL else 'Not configured'}")

# Log message tag definitions
logger.info("Message tag definitions:")
for tag_name in dir(MessageTags):
    if not tag_name.startswith('__'):
        tag_value = getattr(MessageTags, tag_name)
        logger.info(f"  {tag_name} = {tag_value}")

# Initialize AWS clients with retry configuration
try:
    session = boto3.Session(region_name=AWS_REGION)
    sqs_client = session.client('sqs', 
                            config=boto3.session.Config(
                                retries={'max_attempts': 5, 'mode': 'standard'},
                                connect_timeout=5,
                                read_timeout=10
                            ))
    s3_client = session.client('s3', 
                            config=boto3.session.Config(
                                retries={'max_attempts': 5, 'mode': 'standard'},
                                connect_timeout=5,
                                read_timeout=10
                            ))
    logger.info("AWS clients initialized with retry configuration")
except Exception as e:
    logger.critical(f"Failed to initialize AWS clients: {str(e)}")
    logger.critical(traceback.format_exc())
    sys.exit(1)

class Indexer:
    def __init__(self):
        self.schema = Schema(
            url=ID(stored=True),
            content=TEXT(stored=True)
        )
        self.temp_dir = tempfile.mkdtemp()
        self.index_dir = os.path.join(self.temp_dir, "index")
        os.makedirs(self.index_dir, exist_ok=True)
        self.hostname = socket.gethostname()
        self.indexer_id = hashlib.md5(self.hostname.encode()).hexdigest()[:8]
        
        # Error tracking and circuit breaker
        self.error_count = 0
        self.error_threshold = int(os.getenv('ERROR_THRESHOLD', '5'))
        self.backoff_time = int(os.getenv('BACKOFF_TIME', '60'))  # seconds
        self.last_backoff_time = None
        
        # Stats
        self.start_time = datetime.datetime.now()
        self.documents_indexed = 0
        self.failed_documents = 0
        
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
        logger.info(f"Indexer initialized with ID: {self.indexer_id} on host {self.hostname}")
        
        # Load master index file or create new one
        self.master_index = self._load_master_index()

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
            return {
                "last_updated": datetime.datetime.now().isoformat(),
                "document_count": 0,
                "documents": {},
                "keywords": {}
            }

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

    def send_to_dlq(self, message_body, error_details):
        """Send failed message to Dead Letter Queue"""
        if not SQS_DLQ_URL:
            logger.warning(f"No DLQ configured, discarding failed message")
            return False
            
        try:
            enriched_message = {
                'original_message': message_body,
                'error_details': error_details,
                'indexer_id': self.indexer_id,
                'hostname': self.hostname,
                'timestamp': datetime.datetime.now().isoformat(),
                'tag': MessageTags.STATUS_ERROR  # Add message tag
            }
            
            response = sqs_client.send_message(
                QueueUrl=SQS_DLQ_URL,
                MessageBody=json.dumps(enriched_message)
            )
            logger.info(f"Sent failed message to DLQ: {response.get('MessageId')}")
            return True
        except Exception as e:
            logger.error(f"Failed to send to DLQ: {str(e)}")
            return False

    def index_document(self, url, content, tag=MessageTags.CONTENT_TEXT):
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
                "tag": tag,  # Store the message tag
                "indexer_id": self.indexer_id,
                "hostname": self.hostname
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
                "tag": tag
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
            
            # Update stats
            self.documents_indexed += 1
            
            # Send success status message
            try:
                status_message = {
                    'url': url,
                    'doc_id': doc_id,
                    'indexer_id': self.indexer_id,
                    'timestamp': datetime.datetime.now().isoformat(),
                    'tag': MessageTags.STATUS_SUCCESS,
                    'message': f"Successfully indexed document with {len(keywords)} keywords"
                }
                sqs_client.send_message(
                    QueueUrl=SQS_QUEUE_URL,
                    MessageBody=json.dumps(status_message)
                )
            except Exception as e:
                logger.warning(f"Could not send success status: {str(e)}")
            
            logger.info(f"Successfully indexed document with optimized search: {url}")
            
        except Exception as e:
            logger.error(f"Error indexing document from {url}: {str(e)}")
            logger.error(traceback.format_exc())
            self.failed_documents += 1
            
            try:
                self.writer.cancel()
            except:
                pass
            self.writer = self.index.writer()  # Create new writer after error
            
            # Send error status message
            try:
                error_message = {
                    'url': url,
                    'indexer_id': self.indexer_id,
                    'timestamp': datetime.datetime.now().isoformat(),
                    'tag': MessageTags.STATUS_ERROR,
                    'error': str(e),
                    'message': f"Failed to index document: {str(e)}"
                }
                sqs_client.send_message(
                    QueueUrl=SQS_QUEUE_URL,
                    MessageBody=json.dumps(error_message)
                )
            except Exception as e2:
                logger.warning(f"Could not send error status: {str(e2)}")

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
                    results.append(self.master_index["documents"][doc_id]["url"])
            
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
            logger.error(traceback.format_exc())
            return []

    def process_sqs_message(self, message):
        try:
            body = json.loads(message['Body'])
            
            # Check for message tag to determine processing
            message_tag = body.get('tag', MessageTags.CONTENT_TEXT)  # Default to content if no tag
            
            # Handle different message types
            if message_tag == MessageTags.HEARTBEAT:
                # Just log heartbeat messages
                crawler_id = body.get('crawler_id', 'unknown')
                status = body.get('status', 'unknown')
                logger.info(f"Received heartbeat from crawler {crawler_id}: {status}")
                return True
                
            elif message_tag == MessageTags.SYSTEM_COMMAND:
                # Handle system commands
                command = body.get('command', '')
                logger.info(f"Received system command: {command}")
                
                if command == 'shutdown':
                    logger.info("Shutdown command received, exiting...")
                    # Clean up and exit
                    sys.exit(0)
                    
                return True
                
            elif message_tag == MessageTags.SHUTDOWN:
                # Handle shutdown signal
                logger.info("Shutdown signal received, exiting...")
                sys.exit(0)
                
            elif message_tag in [MessageTags.CONTENT_TEXT, MessageTags.CONTENT_HTML, MessageTags.CONTENT_MEDIA]:
                # Process regular content
                url = body.get('url')
                content = body.get('content')
                source = body.get('crawler_id', 'unknown')
                
                if not url or not content:
                    logger.warning(f"Message missing required fields: {body}")
                    return False
                
                logger.info(f"Processing content from {url} (source: {source}, tag: {message_tag})")
                self.index_document(url, content, message_tag)
                return True
                
            else:
                # Unknown message type
                logger.warning(f"Received message with unknown tag: {message_tag}")
                return True
                
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding message: {str(e)}")
            logger.error(f"Raw message body: {message.get('Body', 'No body')}")
            return False
        except KeyError as e:
            logger.error(f"Missing key in message: {str(e)}")
            logger.error(f"Message body: {message.get('Body', 'No body')}")
            return False
        except Exception as e:
            logger.error(f"Error processing message: {str(e)}")
            logger.error(traceback.format_exc())
            return False

    def health_check(self):
        """Report indexer health metrics"""
        uptime = datetime.datetime.now() - self.start_time
        
        health_data = {
            'indexer_id': self.indexer_id,
            'hostname': self.hostname,
            'uptime_seconds': uptime.total_seconds(),
            'documents_indexed': self.documents_indexed,
            'failed_documents': self.failed_documents,
            'indexed_urls_count': self.master_index.get('document_count', 0),
            'error_count': self.error_count,
            'status': 'healthy' if self.error_count < self.error_threshold else 'degraded'
        }
        
        logger.info(f"Health stats: {json.dumps(health_data)}")
        
        # Send heartbeat message
        try:
            heartbeat = {
                'indexer_id': self.indexer_id,
                'hostname': self.hostname,
                'timestamp': datetime.datetime.now().isoformat(),
                'tag': MessageTags.HEARTBEAT,
                'status': 'running',
                'health': health_data
            }
            sqs_client.send_message(
                QueueUrl=SQS_QUEUE_URL,
                MessageBody=json.dumps(heartbeat)
            )
            logger.info(f"Sent heartbeat with tag {MessageTags.HEARTBEAT}")
        except Exception as e:
            logger.warning(f"Could not send heartbeat: {str(e)}")
            
        return health_data

    def indexer_loop(self):
        health_check_interval = 50  # Report health every 50 iterations
        iteration = 0
        
        # Send startup heartbeat
        try:
            heartbeat = {
                'indexer_id': self.indexer_id,
                'hostname': self.hostname,
                'timestamp': datetime.datetime.now().isoformat(),
                'tag': MessageTags.HEARTBEAT,
                'status': 'starting'
            }
            sqs_client.send_message(
                QueueUrl=SQS_QUEUE_URL,
                MessageBody=json.dumps(heartbeat)
            )
            logger.info(f"Sent startup heartbeat with tag {MessageTags.HEARTBEAT}")
        except Exception as e:
            logger.warning(f"Could not send startup heartbeat: {str(e)}")
        
        while True:
            try:
                # Periodically check health
                iteration += 1
                if iteration % health_check_interval == 0:
                    self.health_check()
                
                # Receive messages from SQS
                response = sqs_client.receive_message(
                    QueueUrl=SQS_QUEUE_URL,
                    MaxNumberOfMessages=10,
                    WaitTimeSeconds=20,
                    VisibilityTimeout=30
                )

                if 'Messages' in response:
                    logger.info(f"Received {len(response['Messages'])} messages")
                    
                    for message in response['Messages']:
                        receipt_handle = message['ReceiptHandle']
                        processed = self.process_sqs_message(message)
                        
                        # Delete message if processed successfully
                        if processed:
                            try:
                                sqs_client.delete_message(
                                    QueueUrl=SQS_QUEUE_URL,
                                    ReceiptHandle=receipt_handle
                                )
                                logger.info("Successfully processed and deleted message")
                            except Exception as e:
                                logger.error(f"Error deleting message: {str(e)}")
                        else:
                            # Send to DLQ if processing failed
                            try:
                                self.send_to_dlq(message.get('Body', ''), "Failed to process message")
                                # Then delete from main queue
                                sqs_client.delete_message(
                                    QueueUrl=SQS_QUEUE_URL,
                                    ReceiptHandle=receipt_handle
                                )
                                logger.info("Sent failed message to DLQ and deleted from main queue")
                            except Exception as e:
                                logger.error(f"Error handling failed message: {str(e)}")
                                
                                # Return message to queue for retry
                                try:
                                    sqs_client.change_message_visibility(
                                        QueueUrl=SQS_QUEUE_URL,
                                        ReceiptHandle=receipt_handle,
                                        VisibilityTimeout=0
                                    )
                                except Exception as e:
                                    logger.error(f"Error returning message to queue: {str(e)}")
                else:
                    logger.info("No messages received")

            except Exception as e:
                logger.error(f"Error in indexer loop: {str(e)}")
                logger.error(traceback.format_exc())
                self.error_count += 1
                time.sleep(5)  # Wait before retrying

def indexer_process():
    logger.info("Indexer node started")
    
    try:
        indexer = Indexer()
        indexer.indexer_loop()
    except KeyboardInterrupt:
        logger.info("Indexer shutting down due to keyboard interrupt")
    except Exception as e:
        logger.critical(f"Fatal error in indexer process: {str(e)}")
        logger.critical(traceback.format_exc())
        sys.exit(1)

if __name__ == '__main__':
    # Check if we're sending a command
    if len(sys.argv) > 1 and sys.argv[1] == 'command':
        if len(sys.argv) < 3:
            print("Usage: python indexer_node.py command [shutdown|status]")
            sys.exit(1)
            
        command = sys.argv[2]
        
        try:
            # Initialize SQS client
            session = boto3.Session(region_name=AWS_REGION)
            sqs_client = session.client('sqs')
            
            if command == 'shutdown':
                # Send shutdown command to all indexers
                message = {
                    'tag': MessageTags.SHUTDOWN,
                    'timestamp': datetime.datetime.now().isoformat(),
                    'command': 'shutdown',
                    'sender': socket.gethostname()
                }
                response = sqs_client.send_message(
                    QueueUrl=SQS_QUEUE_URL,
                    MessageBody=json.dumps(message)
                )
                print(f"Shutdown command sent. MessageId: {response.get('MessageId')}")
                
            elif command == 'status':
                # Send a command to get status from all indexers
                message = {
                    'tag': MessageTags.SYSTEM_COMMAND,
                    'command': 'report-status',
                    'timestamp': datetime.datetime.now().isoformat(),
                    'sender': socket.gethostname()
                }
                response = sqs_client.send_message(
                    QueueUrl=SQS_QUEUE_URL,
                    MessageBody=json.dumps(message)
                )
                print(f"Status command sent. MessageId: {response.get('MessageId')}")
                
            else:
                print(f"Unknown command: {command}")
                print("Available commands: shutdown, status")
                sys.exit(1)
                
            sys.exit(0)
            
        except Exception as e:
            print(f"Error sending command: {str(e)}")
            traceback.print_exc()
            sys.exit(1)
            
    indexer_process() 
