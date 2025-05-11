from flask import Flask, render_template, request, jsonify, redirect, url_for
import boto3
import os
import logging
import json
from dotenv import load_dotenv
from indexer_node import Indexer
import tempfile
from celery import Celery
from celery_config import app as celery_app

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('WebApp')

# AWS Configuration
AWS_REGION = os.getenv('AWS_REGION', 'eu-north-1')
SQS_QUEUE_URL = os.getenv('SQS_QUEUE_URL')
SQS_URLS_QUEUE = os.getenv('SQS_URLS_QUEUE')

# Initialize AWS clients
sqs_client = boto3.client('sqs', region_name=AWS_REGION)

# Initialize Flask app
app = Flask(__name__)

# Initialize indexer for search
temp_dir = tempfile.mkdtemp()
indexer = Indexer()

@app.route('/')
def index():
    """Home page with system status and controls"""
    return render_template('index.html')

@app.route('/add-url', methods=['POST'])
def add_url():
    """Add a new URL to crawl"""
    url = request.form.get('url')
    if url:
        try:
            # Send URL to the queue
            sqs_client.send_message(
                QueueUrl=SQS_URLS_QUEUE,
                MessageBody=json.dumps({'url': url})
            )
            logger.info(f"Added URL to queue: {url}")
            return jsonify({"status": "success", "message": f"Added {url} to crawl queue"})
        except Exception as e:
            logger.error(f"Error adding URL: {str(e)}")
            return jsonify({"status": "error", "message": str(e)}), 500
    return jsonify({"status": "error", "message": "No URL provided"}), 400

@app.route('/search')
def search():
    """Search the indexed content"""
    query = request.args.get('query', '')
    if query:
        results = indexer.search(query)
        return jsonify({"results": results})
    return jsonify({"results": []})

@app.route('/status')
def status():
    """Get system status"""
    try:
        # Get queue attributes
        urls_queue_attrs = sqs_client.get_queue_attributes(
            QueueUrl=SQS_URLS_QUEUE,
            AttributeNames=['ApproximateNumberOfMessages']
        )
        
        content_queue_attrs = sqs_client.get_queue_attributes(
            QueueUrl=SQS_QUEUE_URL,
            AttributeNames=['ApproximateNumberOfMessages']
        )
        
        urls_queue_size = int(urls_queue_attrs['Attributes']['ApproximateNumberOfMessages'])
        content_queue_size = int(content_queue_attrs['Attributes']['ApproximateNumberOfMessages'])
        
        status_data = {
            "urls_queue_size": urls_queue_size,
            "content_queue_size": content_queue_size
        }
        
        return jsonify(status_data)
    except Exception as e:
        logger.error(f"Error getting status: {str(e)}")
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    # Create templates directory if it doesn't exist
    os.makedirs('templates', exist_ok=True)
    os.makedirs('static', exist_ok=True)
    
    app.run(debug=True, host='0.0.0.0', port=5000)
