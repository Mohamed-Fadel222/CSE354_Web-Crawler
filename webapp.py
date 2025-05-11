from flask import Flask, render_template, request, jsonify, redirect, url_for
import boto3
import os
import logging
import json
from dotenv import load_dotenv
import tempfile
import io
import base64
import time

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

logger.info(f"AWS Region: {AWS_REGION}")
logger.info(f"SQS Content Queue: {SQS_QUEUE_URL}")
logger.info(f"SQS URLs Queue: {SQS_URLS_QUEUE}")
logger.info(f"S3 Bucket: {S3_BUCKET_NAME}")

# Initialize AWS clients
sqs_client = boto3.client('sqs', region_name=AWS_REGION)
s3_client = boto3.client('s3', region_name=AWS_REGION)

# Initialize Flask app
app = Flask(__name__)

# Global cache for URLs (to avoid repeated S3/SQS queries)
url_cache = []
last_cache_update = 0
CACHE_TTL = 60  # seconds

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

def refresh_url_cache():
    """Update the global URL cache from both SQS and S3"""
    global url_cache, last_cache_update
    
    # Check if cache is still fresh
    current_time = time.time()
    if current_time - last_cache_update < CACHE_TTL and url_cache:
        return url_cache
    
    new_cache = []
    logger.info("Refreshing URL cache")
    
    # 1. Get URLs from content queue
    try:
        response = sqs_client.receive_message(
            QueueUrl=SQS_QUEUE_URL,
            MaxNumberOfMessages=10,
            VisibilityTimeout=5,
            WaitTimeSeconds=1
        )
        
        if 'Messages' in response:
            for message in response['Messages']:
                try:
                    receipt = message['ReceiptHandle']
                    body = json.loads(message['Body'])
                    
                    if 'url' in body:
                        new_cache.append({
                            'url': body['url'],
                            'source': 'SQS Content Queue'
                        })
                        logger.info(f"Found URL in SQS: {body['url']}")
                    
                    # Return message to queue
                    sqs_client.change_message_visibility(
                        QueueUrl=SQS_QUEUE_URL,
                        ReceiptHandle=receipt,
                        VisibilityTimeout=0
                    )
                except Exception as e:
                    logger.error(f"Error processing SQS message: {str(e)}")
    except Exception as e:
        logger.error(f"Error accessing SQS content queue: {str(e)}")
    
    # 2. Get the newest content.json files from S3 (most likely to contain actual indexed content)
    try:
        # List objects sorted by date (newest first)
        response = s3_client.list_objects_v2(
            Bucket=S3_BUCKET_NAME,
            Prefix='content/'  # Try content directory first
        )
        
        if 'Contents' not in response or len(response['Contents']) == 0:
            # If no content folder, try at root level
            response = s3_client.list_objects_v2(
                Bucket=S3_BUCKET_NAME
            )
        
        # Sort by last modified, newest first
        if 'Contents' in response:
            sorted_objects = sorted(
                response['Contents'], 
                key=lambda obj: obj.get('LastModified', 0),
                reverse=True
            )
            
            # Process the 20 newest objects
            for obj in sorted_objects[:20]:
                key = obj['Key']
                
                # Skip binary files
                if key.endswith('.seg') or key.endswith('.dat') or '_MAIN_' in key:
                    continue
                    
                # Focus on JSON files first as they're most likely to have structured data
                if key.endswith('.json') or key.endswith('.txt') or '.' not in key:
                    try:
                        obj_response = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=key)
                        content = obj_response['Body'].read().decode('utf-8', errors='ignore')
                        
                        # Try to parse as JSON
                        try:
                            data = json.loads(content)
                            if isinstance(data, dict) and 'url' in data:
                                new_cache.append({
                                    'url': data['url'],
                                    'source': f'S3:{key}'
                                })
                                logger.info(f"Found URL in S3 JSON: {data['url']}")
                        except json.JSONDecodeError:
                            # Not JSON, look for URLs in the text
                            if 'http://' in content or 'https://' in content:
                                for line in content.split('\n'):
                                    if 'http://' in line or 'https://' in line:
                                        url_start = line.find('http')
                                        url_candidate = line[url_start:].split()[0].strip()
                                        # Basic URL validation
                                        if ' ' not in url_candidate and url_candidate.startswith('http'):
                                            new_cache.append({
                                                'url': url_candidate,
                                                'source': f'S3:{key} (text)'
                                            })
                                            logger.info(f"Found URL in S3 text: {url_candidate}")
                                            break
                    except Exception as e:
                        logger.warning(f"Error processing S3 object {key}: {str(e)}")
                        continue
    except Exception as e:
        logger.error(f"Error accessing S3 bucket: {str(e)}")
    
    # 3. As a fallback, also check URLs queue
    try:
        response = sqs_client.receive_message(
            QueueUrl=SQS_URLS_QUEUE,
            MaxNumberOfMessages=10,
            VisibilityTimeout=5,
            WaitTimeSeconds=1
        )
        
        if 'Messages' in response:
            for message in response['Messages']:
                try:
                    receipt = message['ReceiptHandle']
                    body = json.loads(message['Body'])
                    
                    if 'url' in body:
                        new_cache.append({
                            'url': body['url'],
                            'source': 'SQS URLs Queue'
                        })
                        logger.info(f"Found URL in URLs queue: {body['url']}")
                    
                    # Return message to queue
                    sqs_client.change_message_visibility(
                        QueueUrl=SQS_URLS_QUEUE,
                        ReceiptHandle=receipt,
                        VisibilityTimeout=0
                    )
                except Exception as e:
                    logger.error(f"Error processing URLs queue message: {str(e)}")
    except Exception as e:
        logger.error(f"Error accessing URLs queue: {str(e)}")
    
    # Update the cache
    url_cache = new_cache
    last_cache_update = current_time
    logger.info(f"URL cache refreshed with {len(url_cache)} URLs")
    return url_cache

@app.route('/search')
def search():
    """Fast search using the optimized index structure in S3"""
    query = request.args.get('query', '').lower()
    max_results = int(request.args.get('max_results', '3'))  # Limit to 3 results by default
    logger.info(f"Searching for: {query} (max results: {max_results})")
    
    results = []
    
    try:
        # First try to load the master index for fastest search
        try:
            logger.info("Loading master index for fast search")
            response = s3_client.get_object(
                Bucket=S3_BUCKET_NAME, 
                Key='searchable_index/master_index.json'
            )
            master_index = json.loads(response['Body'].read().decode('utf-8'))
            logger.info(f"Loaded master index with {master_index.get('document_count', 0)} documents")
            
            # Use the keyword-based fast search
            query_words = query.lower().split()
            matching_docs = {}  # Use a dict to track relevance scores
            
            # Check if any keywords match
            for word in query_words:
                if len(word) > 3 and word in master_index.get("keywords", {}):
                    for doc_id in master_index["keywords"][word]:
                        # Increase relevance score for each keyword match
                        if doc_id not in matching_docs:
                            matching_docs[doc_id] = 1
                        else:
                            matching_docs[doc_id] += 1
                    logger.info(f"Keyword '{word}' matched {len(master_index['keywords'][word])} documents")
            
            # Sort docs by relevance score (most matches first)
            sorted_docs = sorted(matching_docs.items(), key=lambda x: x[1], reverse=True)
            logger.info(f"Found {len(sorted_docs)} documents with relevant keywords")
            
            # Get top results
            top_docs = sorted_docs[:max_results]
            
            # Get the document content and URLs
            for doc_id, score in top_docs:
                if doc_id in master_index.get("documents", {}):
                    try:
                        # Retrieve the full document content from S3
                        doc_response = s3_client.get_object(
                            Bucket=S3_BUCKET_NAME,
                            Key=f'searchable_index/documents/{doc_id}.json'
                        )
                        document = json.loads(doc_response['Body'].read().decode('utf-8'))
                        
                        results.append({
                            'url': document['url'],
                            'content': document.get('content', ''),
                            'source': 'keyword_search',
                            'relevance': score
                        })
                        logger.info(f"Found document with URL: {document['url']} (relevance: {score})")
                    except Exception as e:
                        # If we can't get the content, at least return the URL
                        logger.warning(f"Error retrieving document {doc_id}: {str(e)}")
                        results.append({
                            'url': master_index["documents"][doc_id]["url"],
                            'content': '',
                            'source': 'master_index_only',
                            'relevance': score
                        })
            
            logger.info(f"Fast keyword search found {len(results)} top-ranking results")
            
            # If no results from keyword search, try full content search
            if not results:
                logger.info("No results from keyword search, trying full content search")
                
                # Load and search individual document files
                for doc_id in master_index.get("documents", {})[:20]:  # Limit to first 20 docs
                    try:
                        doc_response = s3_client.get_object(
                            Bucket=S3_BUCKET_NAME,
                            Key=f'searchable_index/documents/{doc_id}.json'
                        )
                        document = json.loads(doc_response['Body'].read().decode('utf-8'))
                        
                        # Check if query appears in content and calculate relevance
                        content = document.get('content', '').lower()
                        if content and query in content:
                            # Simple relevance: frequency of query in content
                            relevance = content.count(query)
                            results.append({
                                'url': document['url'],
                                'content': document.get('content', ''),
                                'source': 'full_content_search',
                                'relevance': relevance
                            })
                            logger.info(f"Full content match in document: {document['url']} (relevance: {relevance})")
                    except Exception as e:
                        logger.warning(f"Error loading document {doc_id}: {str(e)}")
                        continue
                
                # Sort by relevance and take top results
                results = sorted(results, key=lambda x: x.get('relevance', 0), reverse=True)[:max_results]
                
        except Exception as e:
            logger.warning(f"Error using master index: {str(e)}")
        
        # If still no results, fall back to direct S3 search (less relevant)
        if not results:
            logger.info("No results from optimized index, trying direct S3 search")
            
            # Try to list all objects in the documents directory
            response = s3_client.list_objects_v2(
                Bucket=S3_BUCKET_NAME,
                Prefix='searchable_index/documents/'
            )
            
            if 'Contents' in response:
                logger.info(f"Found {len(response['Contents'])} documents in S3")
                
                # Look through document files
                for obj in response['Contents'][:20]:  # Limit to 20 docs for performance
                    key = obj['Key']
                    
                    if not key.endswith('.json'):
                        continue
                    
                    try:
                        obj_response = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=key)
                        content = obj_response['Body'].read().decode('utf-8')
                        
                        # Try parsing as JSON
                        try:
                            document = json.loads(content)
                            if 'url' in document and 'content' in document:
                                if query.lower() in document['content'].lower():
                                    results.append({
                                        'url': document['url'],
                                        'content': document.get('content', ''),
                                        'source': 'direct_s3_search'
                                    })
                                    logger.info(f"Direct search match: {document['url']}")
                        except json.JSONDecodeError:
                            continue
                    except Exception as e:
                        logger.warning(f"Error processing document {key}: {str(e)}")
                        continue
        
        # If we still have no results, fall back to searching in any S3 object
        if not results:
            logger.info("Trying fallback search in any S3 object")
            
            # Get a list of all objects
            all_objects = []
            paginator = s3_client.get_paginator('list_objects_v2')
            
            for page in paginator.paginate(Bucket=S3_BUCKET_NAME):
                if 'Contents' in page:
                    for obj in page['Contents']:
                        if not (obj['Key'].endswith('.seg') or '_MAIN_' in obj['Key']):
                            all_objects.append(obj)
            
            # Sort by last modified (newest first)
            all_objects.sort(key=lambda x: x.get('LastModified', ''), reverse=True)
            
            # Check the 20 newest files first
            for obj in all_objects[:20]:
                key = obj['Key']
                
                try:
                    obj_response = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=key)
                    content = obj_response['Body'].read().decode('utf-8', errors='ignore')
                    
                    if query.lower() in content.lower():
                        # Try to extract URL from content
                        url = None
                        
                        # Try as JSON first
                        try:
                            data = json.loads(content)
                            if isinstance(data, dict) and 'url' in data:
                                url = data['url']
                                # If this is a structured document, we want to include full content
                                results.append({
                                    'url': url,
                                    'content': data.get('content', content[:1000]),
                                    'source': f'fallback_json:{key}'
                                })
                                logger.info(f"Fallback JSON search match: {url}")
                                continue
                        except:
                            pass
                        
                        # If not JSON, look for URL in text
                        if not url and ('http://' in content or 'https://' in content):
                            for line in content.split('\n'):
                                if 'http://' in line or 'https://' in line:
                                    start = line.find('http')
                                    end = len(line)
                                    for i in range(start, len(line)):
                                        if i >= len(line) or line[i] in ' "\'\n\r\t<>':
                                            end = i
                                            break
                                    url = line[start:end]
                                    if url:
                                        break
                        
                        # Use S3 object key if we still don't have a URL
                        if not url:
                            url = f"S3:{key}"
                        
                        # Add to results if not already there
                        url_list = [r['url'] for r in results]
                        if url and url not in url_list:
                            results.append({
                                'url': url,
                                'content': content[:1000],
                                'source': f'fallback_text:{key}'
                            })
                            logger.info(f"Fallback text search match: {url}")
                        
                        # Stop at 10 results
                        if len(results) >= 10:
                            break
                except Exception as e:
                    logger.warning(f"Error in fallback search for {key}: {str(e)}")
                    continue
    
    except Exception as e:
        logger.error(f"Error in search: {str(e)}")
    
    # Don't check SQS queues as requested by user, focus only on S3 content
    
    # Return URL and content for each result
    logger.info(f"Returning {len(results)} top search results")
    response = jsonify({"results": results})
    
    # Add CORS headers to prevent issues with ngrok
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type')
    
    return response

@app.route('/status')
def status():
    """Get system status"""
    try:
        # Force a fresh request for queue attributes (no caching)
        urls_queue_attrs = sqs_client.get_queue_attributes(
            QueueUrl=SQS_URLS_QUEUE,
            AttributeNames=['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible']
        )
        
        content_queue_attrs = sqs_client.get_queue_attributes(
            QueueUrl=SQS_QUEUE_URL,
            AttributeNames=['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible']
        )
        
        # Get both visible and in-flight messages
        urls_queue_visible = int(urls_queue_attrs['Attributes']['ApproximateNumberOfMessages'])
        urls_queue_inflight = int(urls_queue_attrs['Attributes']['ApproximateNumberOfMessagesNotVisible'])
        urls_queue_size = urls_queue_visible + urls_queue_inflight
        
        content_queue_visible = int(content_queue_attrs['Attributes']['ApproximateNumberOfMessages'])
        content_queue_inflight = int(content_queue_attrs['Attributes']['ApproximateNumberOfMessagesNotVisible'])
        content_queue_size = content_queue_visible + content_queue_inflight
        
        # Log the actual values for debugging
        logger.info(f"URLs Queue: {urls_queue_visible} visible + {urls_queue_inflight} in flight = {urls_queue_size} total")
        logger.info(f"Content Queue: {content_queue_visible} visible + {content_queue_inflight} in flight = {content_queue_size} total")
        
        # Check S3 bucket status
        try:
            s3_client.head_bucket(Bucket=S3_BUCKET_NAME)
            s3_status = "available"
            
            # Count indexed documents in S3
            indexed_docs = 0
            try:
                response = s3_client.list_objects_v2(
                    Bucket=S3_BUCKET_NAME,
                    Prefix='searchable_index/documents/'
                )
                if 'Contents' in response:
                    indexed_docs = len(response['Contents'])
            except:
                pass
                
        except Exception:
            s3_status = "unavailable"
            indexed_docs = 0
            
        status_data = {
            "urls_queue_size": urls_queue_size,
            "content_queue_size": content_queue_size,
            "urls_queue_visible": urls_queue_visible,
            "urls_queue_inflight": urls_queue_inflight,
            "content_queue_visible": content_queue_visible,
            "content_queue_inflight": content_queue_inflight,
            "s3_status": s3_status,
            "indexed_docs": indexed_docs,
            "timestamp": time.time()
        }
        
        # Add CORS headers to prevent issues with ngrok
        response = jsonify(status_data)
        response.headers.add('Access-Control-Allow-Origin', '*')
        response.headers.add('Access-Control-Allow-Headers', 'Content-Type')
        
        return response
    except Exception as e:
        logger.error(f"Error getting status: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/list-urls')
def list_urls():
    """List all available URLs from cache for direct access"""
    urls = refresh_url_cache()
    return jsonify({
        "urls": [entry['url'] for entry in urls],
        "count": len(urls)
    })

# DIAGNOSTIC AND TEST ENDPOINTS

@app.route('/test/insert-example', methods=['GET'])
def insert_example():
    """Insert test example data into the system"""
    try:
        # 1. Add example.com to the URLs queue
        url_response = sqs_client.send_message(
            QueueUrl=SQS_URLS_QUEUE,
            MessageBody=json.dumps({'url': 'https://example.com/'})
        )
        
        # 2. Add example content directly to the content queue
        content_response = sqs_client.send_message(
            QueueUrl=SQS_QUEUE_URL,
            MessageBody=json.dumps({
                'url': 'https://example.com/',
                'content': 'Example Domain. This domain is for use in illustrative examples.'
            })
        )
        
        # 3. Upload a test file to S3
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key='test-example.json',
            Body=json.dumps({
                'url': 'https://example.com/',
                'content': 'Example Domain test file in S3.'
            })
        )
        
        return jsonify({
            "status": "success",
            "message": "Test data inserted",
            "url_message_id": url_response.get('MessageId'),
            "content_message_id": content_response.get('MessageId')
        })
    except Exception as e:
        logger.error(f"Error inserting test data: {str(e)}")
        return jsonify({"status": "error", "error": str(e)}), 500

@app.route('/test/aws-resources')
def list_aws_resources():
    """List all AWS resources for debugging"""
    resources = {}
    
    try:
        # List all S3 buckets
        buckets = s3_client.list_buckets()
        resources['s3_buckets'] = [bucket['Name'] for bucket in buckets.get('Buckets', [])]
        
        # List all objects in our S3 bucket
        try:
            objects = s3_client.list_objects_v2(Bucket=S3_BUCKET_NAME)
            resources['s3_objects'] = [obj['Key'] for obj in objects.get('Contents', [])]
            resources['s3_object_count'] = len(resources['s3_objects'])
        except Exception as e:
            resources['s3_bucket_error'] = str(e)
        
        # List SQS queues
        sqs = boto3.client('sqs', region_name=AWS_REGION)
        queues = sqs.list_queues()
        resources['sqs_queues'] = queues.get('QueueUrls', [])
        
        # Get messages from content queue (peek only)
        try:
            content_messages = sqs_client.receive_message(
                QueueUrl=SQS_QUEUE_URL,
                MaxNumberOfMessages=10,
                VisibilityTimeout=5,
                WaitTimeSeconds=1
            )
            
            if 'Messages' in content_messages:
                resources['content_queue_messages'] = []
                for msg in content_messages['Messages']:
                    body = json.loads(msg['Body'])
                    resources['content_queue_messages'].append({
                        'message_id': msg['MessageId'],
                        'url': body.get('url', 'No URL found')
                    })
                    
                    # Return message to queue
                    sqs_client.change_message_visibility(
                        QueueUrl=SQS_QUEUE_URL,
                        ReceiptHandle=msg['ReceiptHandle'],
                        VisibilityTimeout=0
                    )
                
                resources['content_messages_count'] = len(resources['content_queue_messages'])
        except Exception as e:
            resources['content_queue_error'] = str(e)
            
        # Get messages from URLs queue (peek only)
        try:
            url_messages = sqs_client.receive_message(
                QueueUrl=SQS_URLS_QUEUE,
                MaxNumberOfMessages=10,
                VisibilityTimeout=5,
                WaitTimeSeconds=1
            )
            
            if 'Messages' in url_messages:
                resources['url_queue_messages'] = []
                for msg in url_messages['Messages']:
                    body = json.loads(msg['Body'])
                    resources['url_queue_messages'].append({
                        'message_id': msg['MessageId'],
                        'url': body.get('url', 'No URL found')
                    })
                    
                    # Return message to queue
                    sqs_client.change_message_visibility(
                        QueueUrl=SQS_URLS_QUEUE,
                        ReceiptHandle=msg['ReceiptHandle'],
                        VisibilityTimeout=0
                    )
                
                resources['url_messages_count'] = len(resources['url_queue_messages'])
        except Exception as e:
            resources['url_queue_error'] = str(e)
            
        resources['aws_config'] = {
            'region': AWS_REGION,
            'content_queue_url': SQS_QUEUE_URL,
            'urls_queue_url': SQS_URLS_QUEUE,
            's3_bucket': S3_BUCKET_NAME
        }
        
        return jsonify(resources)
    except Exception as e:
        logger.error(f"Error listing AWS resources: {str(e)}")
        return jsonify({"status": "error", "error": str(e)}), 500

@app.route('/test/direct-search')
def direct_search():
    """Direct search in SQS queues for any content"""
    results = []
    
    # Look at content queue
    try:
        response = sqs_client.receive_message(
            QueueUrl=SQS_QUEUE_URL,
            MaxNumberOfMessages=10,
            VisibilityTimeout=5,
            WaitTimeSeconds=1
        )
        
        if 'Messages' in response:
            logger.info(f"Found {len(response['Messages'])} messages in content queue")
            for message in response['Messages']:
                try:
                    receipt = message['ReceiptHandle']
                    body = json.loads(message['Body'])
                    
                    if 'url' in body:
                        results.append({
                            'url': body['url'],
                            'source': 'Content Queue',
                            'content_preview': body.get('content', '')[:50] + '...'
                        })
                    
                    # Return message to queue
                    sqs_client.change_message_visibility(
                        QueueUrl=SQS_QUEUE_URL,
                        ReceiptHandle=receipt,
                        VisibilityTimeout=0
                    )
                except Exception as e:
                    logger.error(f"Error processing SQS message: {str(e)}")
        else:
            logger.info("No messages found in content queue")
    except Exception as e:
        logger.error(f"Error accessing content queue: {str(e)}")
    
    # Also check URLs queue
    try:
        response = sqs_client.receive_message(
            QueueUrl=SQS_URLS_QUEUE,
            MaxNumberOfMessages=10,
            VisibilityTimeout=5,
            WaitTimeSeconds=1
        )
        
        if 'Messages' in response:
            logger.info(f"Found {len(response['Messages'])} messages in URLs queue")
            for message in response['Messages']:
                try:
                    receipt = message['ReceiptHandle']
                    body = json.loads(message['Body'])
                    
                    if 'url' in body:
                        results.append({
                            'url': body['url'],
                            'source': 'URLs Queue',
                            'message_id': message.get('MessageId', 'unknown')
                        })
                    
                    # Return message to queue
                    sqs_client.change_message_visibility(
                        QueueUrl=SQS_URLS_QUEUE,
                        ReceiptHandle=receipt,
                        VisibilityTimeout=0
                    )
                except Exception as e:
                    logger.error(f"Error processing URLs message: {str(e)}")
        else:
            logger.info("No messages found in URLs queue")
    except Exception as e:
        logger.error(f"Error accessing URLs queue: {str(e)}")
    
    # Add diagnostic info
    queue_info = {
        'content_queue': SQS_QUEUE_URL,
        'urls_queue': SQS_URLS_QUEUE,
        's3_bucket': S3_BUCKET_NAME
    }
    
    return jsonify({
        "results": results,
        "count": len(results),
        "queue_info": queue_info
    })

@app.route('/test/check-aws')
def check_aws():
    """Check AWS credentials and display them in the web interface"""
    results = {
        "aws_config": {
            "region": AWS_REGION,
            "content_queue_url": SQS_QUEUE_URL,
            "urls_queue_url": SQS_URLS_QUEUE,
            "s3_bucket": S3_BUCKET_NAME
        },
        "aws_tests": []
    }
    
    # Test 1: Check SQS Content Queue
    try:
        test_result = {"name": "SQS Content Queue", "status": "unknown"}
        response = sqs_client.get_queue_attributes(
            QueueUrl=SQS_QUEUE_URL,
            AttributeNames=['ApproximateNumberOfMessages']
        )
        msgs = response['Attributes']['ApproximateNumberOfMessages']
        test_result["status"] = "success"
        test_result["messages"] = f"{msgs} messages in queue"
        results["aws_tests"].append(test_result)
    except Exception as e:
        test_result["status"] = "failed"
        test_result["error"] = str(e)
        results["aws_tests"].append(test_result)
    
    # Test 2: Check SQS URLs Queue
    try:
        test_result = {"name": "SQS URLs Queue", "status": "unknown"}
        response = sqs_client.get_queue_attributes(
            QueueUrl=SQS_URLS_QUEUE,
            AttributeNames=['ApproximateNumberOfMessages']
        )
        msgs = response['Attributes']['ApproximateNumberOfMessages']
        test_result["status"] = "success"
        test_result["messages"] = f"{msgs} messages in queue"
        results["aws_tests"].append(test_result)
    except Exception as e:
        test_result["status"] = "failed"
        test_result["error"] = str(e)
        results["aws_tests"].append(test_result)
    
    # Test 3: Check S3 Bucket
    try:
        test_result = {"name": "S3 Bucket", "status": "unknown"}
        response = s3_client.list_objects_v2(
            Bucket=S3_BUCKET_NAME,
            MaxKeys=1
        )
        test_result["status"] = "success"
        if 'Contents' in response:
            test_result["objects"] = f"{len(response['Contents'])} objects found"
        else:
            test_result["objects"] = "Bucket exists but is empty"
        results["aws_tests"].append(test_result)
    except Exception as e:
        test_result["status"] = "failed"
        test_result["error"] = str(e)
        results["aws_tests"].append(test_result)
    
    # Get AWS identity
    try:
        sts_client = boto3.client('sts', region_name=AWS_REGION)
        identity = sts_client.get_caller_identity()
        results["aws_identity"] = {
            "account_id": identity.get('Account', 'Unknown'),
            "user_arn": identity.get('Arn', 'Unknown'),
            "user_id": identity.get('UserId', 'Unknown')
        }
    except Exception as e:
        results["aws_identity_error"] = str(e)
    
    # Return results with HTML formatting for better display
    html = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>AWS Credentials Check</title>
        <style>
            body { font-family: Arial, sans-serif; padding: 20px; }
            .success { color: green; }
            .failed { color: red; }
            .container { max-width: 800px; margin: 0 auto; }
            pre { background: #f4f4f4; padding: 10px; border-radius: 5px; overflow-x: auto; }
            .test { margin-bottom: 15px; border: 1px solid #ddd; padding: 10px; border-radius: 5px; }
            h3 { margin-top: 0; }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>AWS Credentials Check</h1>
            
            <h2>Configuration</h2>
            <pre>%s</pre>
            
            <h2>Tests</h2>
            %s
            
            <h2>Identity</h2>
            <pre>%s</pre>
            
            <p><a href="/">&larr; Back to home</a></p>
        </div>
    </body>
    </html>
    """ % (
        json.dumps(results["aws_config"], indent=4),
        ''.join([
            f"""
            <div class="test">
                <h3>{test['name']}: <span class="{test['status']}">{test['status'].upper()}</span></h3>
                {'<p>' + test.get('messages', '') + '</p>' if test.get('messages') else ''}
                {'<p>' + test.get('objects', '') + '</p>' if test.get('objects') else ''}
                {'<p class="failed">Error: ' + test.get('error', '') + '</p>' if test.get('error') else ''}
            </div>
            """ for test in results["aws_tests"]
        ]),
        json.dumps(results.get("aws_identity", results.get("aws_identity_error", "Unknown")), indent=4)
    )
    
    return html

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
    
    # Initialize URL cache
    refresh_url_cache()
    
    # Start the web server
    try:
        app.run(debug=True, host='0.0.0.0', port=5000)
    except Exception as e:
        logger.error(f"Failed to start web server: {str(e)}")
