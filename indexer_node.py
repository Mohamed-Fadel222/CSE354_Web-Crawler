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
        try:
            self.writer.add_document(url=url, content=content)
            self.writer.commit()
            logger.info(f"Indexed document from {url}")
            self.writer = self.index.writer()  # Create new writer for next document
        except Exception as e:
            logger.error(f"Error indexing document from {url}: {e}")
            self.writer.cancel()
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

def indexer_process():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    
    logger.name = f'Indexer-{rank}'
    logger.info(f"Indexer node started with rank {rank}")
    indexer = Indexer()

    while True:
        try:
            # Receive messages from SQS
            response = sqs_client.receive_message(
                QueueUrl=SQS_QUEUE_URL,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=20
            )

            if 'Messages' in response:
                for message in response['Messages']:
                    try:
                        # Process the message
                        body = json.loads(message['Body'])
                        url = body['url']
                        content = body['content']
                        
                        # Index the document
                        indexer.index_document(url, content)
                        
                        # Delete the message from the queue
                        sqs_client.delete_message(
                            QueueUrl=SQS_QUEUE_URL,
                            ReceiptHandle=message['ReceiptHandle']
                        )
                        
                        logger.info(f"Processed and indexed document from {url}")
                    
                    except json.JSONDecodeError as e:
                        logger.error(f"Error decoding message: {e}")
                        logger.error(f"Message body: {message['Body']}")
                    except KeyError as e:
                        logger.error(f"Missing key in message: {e}")
                        logger.error(f"Message body: {message['Body']}")
                    except Exception as e:
                        logger.error(f"Error processing message: {e}")

            # Check for search requests from master
            if comm.iprobe(source=0, tag=1):  # Tag 1 for search requests
                query = comm.recv(source=0, tag=1)
                results = indexer.search(query)
                comm.send(results, dest=0, tag=2)  # Tag 2 for search results

        except Exception as e:
            logger.error(f"Error in indexer process: {e}")
            time.sleep(5)  # Wait before retrying

if __name__ == '__main__':
    indexer_process() 
