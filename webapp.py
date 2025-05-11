from flask import Flask, render_template, request, redirect, url_for
import boto3, json, os, time
from dotenv import load_dotenv
from whoosh.index import open_dir
from whoosh.qparser import QueryParser
from message_tags import *  # Import all message tags
from botocore.exceptions import ClientError
import logging

# ─── Config ───────────────────────────────────────────────────────────
load_dotenv()
AWS_REGION    = os.getenv('AWS_REGION')
SQS_URLS_QUEUE = os.getenv('SQS_URLS_QUEUE')
SQS_QUEUE_NAME = os.path.basename(SQS_URLS_QUEUE) if SQS_URLS_QUEUE else 'web-crawler-queue'
S3_BUCKET     = os.getenv('S3_BUCKET')

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('Webapp')

# ─── AWS Clients ──────────────────────────────────────────────────────
sqs = boto3.client('sqs', region_name=AWS_REGION)
sqs_resource = boto3.resource('sqs', region_name=AWS_REGION)

def ensure_sqs_queue_exists():
    """Create the SQS queue if it doesn't exist already"""
    global SQS_URLS_QUEUE
    try:
        # Check if queue already exists
        queues = sqs.list_queues(QueueNamePrefix=SQS_QUEUE_NAME)
        if 'QueueUrls' in queues and queues['QueueUrls']:
            SQS_URLS_QUEUE = queues['QueueUrls'][0]
            logger.info(f"Found existing queue: {SQS_URLS_QUEUE}")
            return SQS_URLS_QUEUE
        
        # Create the queue
        response = sqs.create_queue(
            QueueName=SQS_QUEUE_NAME,
            Attributes={
                'VisibilityTimeout': '30',
                'MessageRetentionPeriod': '86400'  # 1 day
            }
        )
        SQS_URLS_QUEUE = response['QueueUrl']
        logger.info(f"Created new SQS queue: {SQS_URLS_QUEUE}")
        return SQS_URLS_QUEUE
    except Exception as e:
        logger.error(f"Error ensuring SQS queue exists: {str(e)}")
        return None

# Ensure SQS queue exists before continuing
SQS_URLS_QUEUE = ensure_sqs_queue_exists()
if not SQS_URLS_QUEUE:
    logger.error("Failed to create or find SQS queue. Check your AWS permissions.")

# ─── Simple Whoosh wrapper for search ─────────────────────────────────
class SimpleIndexer:
    def __init__(self, index_dir='index'):
        self.index = open_dir(index_dir)
    def search(self, text):
        with self.index.searcher() as searcher:
            query   = QueryParser('content', self.index.schema).parse(text)
            results = searcher.search(query, limit=20)
            return [hit['url'] for hit in results]

# For remote search (through SQS if needed)
def search_via_sqs(query_text):
    if not SQS_URLS_QUEUE:
        logger.error("Cannot send search request - SQS queue not available")
        return []
        
    try:
        # Send a search request message
        response = sqs.send_message(
            QueueUrl=SQS_URLS_QUEUE,
            MessageBody=json.dumps({
                'tag': MSG_TAG_SEARCH_REQUEST,
                'query': query_text,
                'timestamp': time.time()
            })
        )
        # In a real system, we would wait for response
        # This is a simplified version
        return []
    except ClientError as e:
        if e.response['Error']['Code'] == 'AWS.SimpleQueueService.NonExistentQueue':
            # Try to recreate queue
            global SQS_URLS_QUEUE
            SQS_URLS_QUEUE = ensure_sqs_queue_exists()
            if SQS_URLS_QUEUE:
                logger.info(f"Recreated queue: {SQS_URLS_QUEUE}")
                # Try again with new queue
                return search_via_sqs(query_text)
        logger.error(f"Error sending search request: {str(e)}")
        return []
    except Exception as e:
        logger.error(f"Error sending search request: {str(e)}")
        return []

indexer = SimpleIndexer()

# ─── Flask app ────────────────────────────────────────────────────────
app = Flask(__name__)

# Helper to check system status via heartbeat messages
def check_system_status():
    if not SQS_URLS_QUEUE:
        return {'status': 'error', 'errors': ['SQS queue not available'], 'heartbeats': []}
        
    try:
        # Check for any error messages
        response = sqs.receive_message(
            QueueUrl=SQS_URLS_QUEUE,
            MaxNumberOfMessages=10,
            MessageAttributeNames=['All'],
            VisibilityTimeout=10,
            WaitTimeSeconds=1
        )
        
        errors = []
        heartbeats = []
        
        if 'Messages' in response:
            for message in response['Messages']:
                try:
                    body = json.loads(message['Body'])
                    tag = body.get('tag')
                    
                    if tag == MSG_TAG_ERROR:
                        errors.append(body.get('message', 'Unknown error'))
                    elif tag == MSG_TAG_HEARTBEAT:
                        heartbeats.append(body.get('message', 'System active'))
                    
                    # Delete the message after processing
                    sqs.delete_message(
                        QueueUrl=SQS_URLS_QUEUE,
                        ReceiptHandle=message['ReceiptHandle']
                    )
                except Exception as e:
                    logger.error(f"Error processing status message: {str(e)}")
        
        return {
            'errors': errors,
            'heartbeats': heartbeats,
            'status': 'error' if errors else 'ok'
        }
    except ClientError as e:
        if e.response['Error']['Code'] == 'AWS.SimpleQueueService.NonExistentQueue':
            # Try to recreate queue
            global SQS_URLS_QUEUE
            SQS_URLS_QUEUE = ensure_sqs_queue_exists()
            if SQS_URLS_QUEUE:
                logger.info(f"Recreated queue: {SQS_URLS_QUEUE}")
                # Try again with new queue
                return check_system_status()
        logger.error(f"Error checking system status: {str(e)}")
        return {'status': 'error', 'errors': [str(e)], 'heartbeats': []}
    except Exception as e:
        logger.error(f"Error checking system status: {str(e)}")
        return {'status': 'unknown', 'errors': [str(e)], 'heartbeats': []}

@app.route('/', methods=['GET','POST'])
def index():
    sent = None
    # Check system status
    status = check_system_status()
    
    if request.method == 'POST':
        if not SQS_URLS_QUEUE:
            # Try to recreate queue
            global SQS_URLS_QUEUE
            SQS_URLS_QUEUE = ensure_sqs_queue_exists()
            if not SQS_URLS_QUEUE:
                status['errors'].append("Cannot submit URLs - SQS queue not available")
                return render_template('index.html', sent=0, system_status=status, error="SQS queue not available")

        # User pastes one URL per line
        urls_text = request.form.get('urls','')
        urls = [u.strip() for u in urls_text.splitlines() if u.strip()]
        sent_count = 0
        
        for url in urls:
            try:
                sqs.send_message(
                    QueueUrl=SQS_URLS_QUEUE,
                    MessageBody=json.dumps({
                        'tag': MSG_TAG_URL_SUBMISSION,
                        'url': url
                    })
                )
                sent_count += 1
            except ClientError as e:
                if e.response['Error']['Code'] == 'AWS.SimpleQueueService.NonExistentQueue':
                    # Try to recreate queue
                    SQS_URLS_QUEUE = ensure_sqs_queue_exists()
                    if SQS_URLS_QUEUE:
                        logger.info(f"Recreated queue: {SQS_URLS_QUEUE}")
                        # Try again with new queue
                        try:
                            sqs.send_message(
                                QueueUrl=SQS_URLS_QUEUE,
                                MessageBody=json.dumps({
                                    'tag': MSG_TAG_URL_SUBMISSION,
                                    'url': url
                                })
                            )
                            sent_count += 1
                        except Exception as e2:
                            logger.error(f"Error sending URL after queue recreation: {str(e2)}")
                else:
                    logger.error(f"Error sending URL to queue: {str(e)}")
            except Exception as e:
                logger.error(f"Error sending URL to queue: {str(e)}")
                
        sent = sent_count
        
    return render_template('index.html', sent=sent, system_status=status)

@app.route('/search', methods=['GET','POST'])
def search():
    results = []
    query   = ''
    if request.method == 'POST':
        query = request.form.get('query','')
        # Local search using Whoosh
        results = indexer.search(query)
        
        # Optionally, you could use the SQS-based search
        # if you want to offload search to a different service
        # remote_results = search_via_sqs(query)
        # results.extend(remote_results)
    return render_template('search.html', query=query, results=results)

@app.route('/system-status')
def system_status():
    status = check_system_status()
    # Pass tag descriptions to the template
    return render_template('status.html', system_status=status, tag_descriptions=TAG_DESCRIPTIONS)

if __name__ == '__main__':
    # Runs on http://localhost:5000
    app.run(debug=True)
