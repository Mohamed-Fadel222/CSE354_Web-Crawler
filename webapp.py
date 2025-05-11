from flask import Flask, render_template, request, jsonify, redirect, url_for
import boto3
import os
import logging
import json
from dotenv import load_dotenv
import tempfile

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('WebApp')

# AWS Configuration
AWS_REGION = os.getenv('AWS_REGION', 'eu-north-1')
SQS_QUEUE_URL = os.getenv('SQS_QUEUE_URL')
SQS_URLS_QUEUE = os.getenv('SQS_URLS_QUEUE')
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')

# Initialize AWS clients
sqs_client = boto3.client('sqs', region_name=AWS_REGION)
s3_client = boto3.client('s3', region_name=AWS_REGION)

# Initialize Flask app
app = Flask(__name__)

@app.route('/')
def index():
    """Home page with system status and controls"""
    logger.info("Rendering index.html template")
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
    """Search indexed content directly from S3"""
    query = request.args.get('query', '')
    if not query:
        return jsonify({"results": []})
    
    try:
        logger.info(f"Searching for '{query}' in S3 bucket")
        
        # List search results folder in S3
        results = []
        try:
            # Get list of all indexed documents
            paginator = s3_client.get_paginator('list_objects_v2')
            
            # Search through S3 objects for the query term
            for page in paginator.paginate(Bucket=S3_BUCKET_NAME, Prefix='index/'):
                if 'Contents' in page:
                    for obj in page['Contents']:
                        try:
                            # Download the object metadata to check for matches
                            key = obj['Key']
                            if key.endswith('.json'):  # Assuming indexed content is stored as JSON
                                response = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=key)
                                content = json.loads(response['Body'].read().decode('utf-8'))
                                
                                # Very simple search - check if query appears in content
                                if 'url' in content and 'content' in content:
                                    if query.lower() in content['content'].lower():
                                        results.append(content['url'])
                                        
                                        # Limit to 10 results to avoid overwhelming response
                                        if len(results) >= 10:
                                            break
                        except Exception as e:
                            logger.error(f"Error processing S3 object {key}: {str(e)}")
                            continue
                            
                    # Break pagination if we have enough results
                    if len(results) >= 10:
                        break
        
            logger.info(f"Found {len(results)} results for '{query}'")
            return jsonify({"results": results})
        except Exception as e:
            logger.error(f"Error searching S3: {str(e)}")
            return jsonify({"results": [], "error": str(e)}), 500
    
    except Exception as e:
        logger.error(f"Error in search: {str(e)}")
        return jsonify({"results": [], "error": str(e)}), 500

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
        
        # Check S3 bucket status
        try:
            s3_client.head_bucket(Bucket=S3_BUCKET_NAME)
            s3_status = "available"
        except Exception:
            s3_status = "unavailable"
            
        status_data = {
            "urls_queue_size": urls_queue_size,
            "content_queue_size": content_queue_size,
            "s3_status": s3_status
        }
        
        return jsonify(status_data)
    except Exception as e:
        logger.error(f"Error getting status: {str(e)}")
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    # Create templates directory if it doesn't exist
    os.makedirs('templates', exist_ok=True)
    os.makedirs('static', exist_ok=True)
    
    logger.info(f"Starting web server on http://localhost:5000")
    
    # Check if templates directory contains index.html
    template_path = os.path.join(os.path.dirname(__file__), 'templates', 'index.html')
    if os.path.exists(template_path):
        logger.info(f"Found template at {template_path}")
    else:
        logger.error(f"Template file not found at {template_path}")
    
    # Start the web server
    try:
        app.run(debug=True, host='0.0.0.0', port=5000)
    except Exception as e:
        logger.error(f"Failed to start web server: {str(e)}")
