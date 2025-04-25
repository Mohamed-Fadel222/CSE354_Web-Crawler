from mpi4py import MPI
import logging
import boto3
import os
from dotenv import load_dotenv
from whoosh.index import create_in, open_dir
from whoosh.fields import Schema, TEXT, ID
from whoosh.qparser import QueryParser
import json
import time

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('Indexer')

# AWS Configuration
AWS_REGION = os.getenv('AWS_REGION', 'eu-north-1')
SQS_QUEUE_URL = os.getenv('SQS_QUEUE_URL')
logger.info(f"Using SQS Queue URL: {SQS_QUEUE_URL}")
logger.info(f"Using AWS Region: {AWS_REGION}")
sqs_client = boto3.client('sqs', region_name=AWS_REGION)

class Indexer:
    def __init__(self):
        self.schema = Schema(
            url=ID(stored=True),
            content=TEXT(stored=True)
        )
        self.index_dir = "index"
        if not os.path.exists(self.index_dir):
            os.mkdir(self.index_dir)
            self.index = create_in(self.index_dir, self.schema)
        else:
            self.index = open_dir(self.index_dir)
        self.writer = self.index.writer()
        logger.info("Indexer initialized")

    def index_document(self, url, content):
        if not content or not url:
            logger.warning(f"Skipping empty document for URL: {url}")
            return

        try:
            self.writer.add_document(url=url, content=content)
            self.writer.commit()
            logger.info(f"Successfully indexed document from {url} ({len(content)} chars)")
            self.writer = self.index.writer()  # Create new writer for next document
        except Exception as e:
            logger.error(f"Error indexing document from {url}: {str(e)}")
            try:
                self.writer.cancel()
            except:
                pass
            self.writer = self.index.writer()  # Create new writer after error

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
            url = body['url']
            content = body['content']
            crawler_rank = body.get('crawler_rank', 'unknown')
            
            logger.info(f"Processing content from {url} (crawler {crawler_rank})")
            self.index_document(url, content)
            
            # Delete the message from the queue
            sqs_client.delete_message(
                QueueUrl=SQS_QUEUE_URL,
                ReceiptHandle=message['ReceiptHandle']
            )
            
            logger.info(f"Successfully processed and deleted message for {url}")
            
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

                # Check for search requests from master
                if self.comm.iprobe(source=0, tag=1):  # Tag 1 for search requests
                    query = self.comm.recv(source=0, tag=1)
                    results = self.search(query)
                    self.comm.send(results, dest=0, tag=2)  # Tag 2 for search results

            except Exception as e:
                logger.error(f"Error in indexer loop: {str(e)}")
                time.sleep(5)  # Wait before retrying

def indexer_process():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    
    logger.name = f'Indexer-{rank}'
    logger.info(f"Indexer node started with rank {rank}")
    indexer = Indexer()

    indexer.comm = comm
    indexer.indexer_loop()

if __name__ == '__main__':
    indexer_process() 
