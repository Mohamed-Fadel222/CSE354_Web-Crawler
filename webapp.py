from flask import Flask, render_template, request, redirect, url_for
import boto3, json, os, time
from dotenv import load_dotenv
from whoosh.index import open_dir
from whoosh.qparser import QueryParser
from message_tags import *  # Import all message tags

# ─── Config ───────────────────────────────────────────────────────────
load_dotenv()
AWS_REGION    = os.getenv('AWS_REGION')
SQS_URLS_QUEUE = os.getenv('SQS_URLS_QUEUE')
S3_BUCKET     = os.getenv('S3_BUCKET')

# ─── Message Tags ─────────────────────────────────────────────────────
# Communication Tags for message differentiation
MSG_TAG_URL_SUBMISSION = 0    # URL to be crawled
MSG_TAG_CONTENT_SUBMISSION = 1  # Content to be indexed
MSG_TAG_SEARCH_REQUEST = 2    # Search request
MSG_TAG_ERROR = 99           # Error/exception
MSG_TAG_HEARTBEAT = 999      # System status/heartbeat

# ─── AWS Clients ──────────────────────────────────────────────────────
sqs = boto3.client('sqs', region_name=AWS_REGION)
# (we'll use S3 below in the crawler)

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
    except Exception as e:
        print(f"Error sending search request: {str(e)}")
        return []

indexer = SimpleIndexer()

# ─── Flask app ────────────────────────────────────────────────────────
app = Flask(__name__)

# Helper to check system status via heartbeat messages
def check_system_status():
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
                    print(f"Error processing status message: {str(e)}")
        
        return {
            'errors': errors,
            'heartbeats': heartbeats,
            'status': 'error' if errors else 'ok'
        }
    except Exception as e:
        print(f"Error checking system status: {str(e)}")
        return {'status': 'unknown', 'errors': [str(e)], 'heartbeats': []}

@app.route('/', methods=['GET','POST'])
def index():
    sent = None
    # Check system status
    status = check_system_status()
    
    if request.method == 'POST':
        # User pastes one URL per line
        urls_text = request.form.get('urls','')
        urls = [u.strip() for u in urls_text.splitlines() if u.strip()]
        for url in urls:
            sqs.send_message(
                QueueUrl=SQS_URLS_QUEUE,
                MessageBody=json.dumps({
                    'tag': MSG_TAG_URL_SUBMISSION,
                    'url': url
                })
            )
        sent = len(urls)
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
