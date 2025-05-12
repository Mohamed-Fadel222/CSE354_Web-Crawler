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
import requests
import hashlib

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

# Message Tags
MSG_TAG_INFO = 0       # Regular informational messages
MSG_TAG_URL = 1        # URL processing messages
MSG_TAG_CONTENT = 2    # Content processing messages  
MSG_TAG_WARNING = 99   # Warning messages
MSG_TAG_ERROR = 999    # Error messages

# Map tags to human-readable names
TAG_NAMES = {
    MSG_TAG_INFO: "Info",
    MSG_TAG_URL: "URL",
    MSG_TAG_CONTENT: "Content",
    MSG_TAG_WARNING: "Warning",
    MSG_TAG_ERROR: "Error"
}

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
    # Check S3 bucket status
    s3_status = "unknown"
    s3_empty = True
    try:
        # Check if bucket exists
        s3_client.head_bucket(Bucket=S3_BUCKET_NAME)
        s3_status = "available"
        
        # Check if bucket has content
        response = s3_client.list_objects_v2(
            Bucket=S3_BUCKET_NAME,
            MaxKeys=5
        )
        s3_empty = 'Contents' not in response or len(response['Contents']) == 0
    except Exception as e:
        logger.error(f"Error checking S3 bucket: {str(e)}")
        s3_status = "unavailable"
    
    logger.info(f"Rendering index.html template (S3 status: {s3_status}, empty: {s3_empty})")
    return render_template('index.html', s3_status=s3_status, s3_empty=s3_empty)

@app.route('/add-url', methods=['POST'])
def add_url():
    """Add a new URL to crawl"""
    url = request.form.get('url')
    if url:
        try:
            # Send URL to the queue with proper tag
            sqs_client.send_message(
                QueueUrl=SQS_URLS_QUEUE,
                MessageBody=json.dumps({
                    'tag': MSG_TAG_URL,
                    'url': url,
                    'source': 'webapp',
                    'timestamp': time.time()
                })
            )
            logger.info(f"Added URL to queue: {url} with tag {MSG_TAG_URL}")
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
                    
                    # Get message tag
                    tag = body.get('tag', MSG_TAG_CONTENT)  # Default to content
                    tag_name = TAG_NAMES.get(tag, "Unknown")
                    
                    if 'url' in body:
                        new_cache.append({
                            'url': body['url'],
                            'source': 'SQS Content Queue',
                            'tag': tag,
                            'tag_name': tag_name
                        })
                        logger.info(f"Found URL in SQS: {body['url']} (Tag: {tag_name})")
                    
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
                                # Get tag if available
                                tag = data.get('tag', MSG_TAG_CONTENT)
                                tag_name = TAG_NAMES.get(tag, "Unknown")
                                
                                new_cache.append({
                                    'url': data['url'],
                                    'source': f'S3:{key}',
                                    'tag': tag,
                                    'tag_name': tag_name
                                })
                                logger.info(f"Found URL in S3 JSON: {data['url']} (Tag: {tag_name})")
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
                                                'source': f'S3:{key} (text)',
                                                'tag': MSG_TAG_URL,
                                                'tag_name': TAG_NAMES[MSG_TAG_URL]
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
                    
                    # Get message tag
                    tag = body.get('tag', MSG_TAG_URL)  # Default to URL tag
                    tag_name = TAG_NAMES.get(tag, "Unknown")
                    
                    if 'url' in body:
                        new_cache.append({
                            'url': body['url'],
                            'source': 'SQS URLs Queue',
                            'tag': tag,
                            'tag_name': tag_name
                        })
                        logger.info(f"Found URL in URLs queue: {body['url']} (Tag: {tag_name})")
                    
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
        # Check if the searchable_index directory exists and create it if needed
        try:
            # Check if searchable_index/documents/ exists
            response = s3_client.list_objects_v2(
                Bucket=S3_BUCKET_NAME,
                Prefix='searchable_index/documents/',
                MaxKeys=1
            )
            
            # If 'Contents' is not in response, the directory doesn't exist
            if 'Contents' not in response:
                logger.warning("searchable_index/documents/ directory not found, might need to add content first")
                
                # Create empty placeholder file to ensure directory exists
                s3_client.put_object(
                    Bucket=S3_BUCKET_NAME,
                    Key='searchable_index/documents/.placeholder',
                    Body=''
                )
                logger.info("Created searchable_index/documents/ directory")
                
                # Create an empty master index
                master_index = {
                    "last_updated": time.time(),
                    "document_count": 0,
                    "documents": {},
                    "keywords": {}
                }
                
                # Save it to S3
                s3_client.put_object(
                    Bucket=S3_BUCKET_NAME,
                    Key='searchable_index/master_index.json',
                    Body=json.dumps(master_index),
                    ContentType='application/json'
                )
                logger.info("Created empty master_index.json")
                
                # Return a message indicating the search index is empty
                return jsonify({
                    "results": [],
                    "message": "The search index is empty. Please add some URLs to crawl first.",
                    "status": "empty_index"
                })
        except Exception as e:
            logger.error(f"Error checking or creating searchable_index directory: {str(e)}")
        
        # First check if S3 bucket exists and has content
        try:
            # Check if there's any content in the bucket
            response = s3_client.list_objects_v2(
                Bucket=S3_BUCKET_NAME,
                MaxKeys=5
            )
            
            # If bucket is empty, return a clear message
            if 'Contents' not in response or len(response['Contents']) == 0:
                logger.warning(f"S3 bucket {S3_BUCKET_NAME} appears to be empty")
                return jsonify({
                    "results": [],
                    "message": "No content available - the database appears to be empty.",
                    "status": "no_content" 
                })
        except Exception as e:
            logger.error(f"Error checking S3 bucket state: {str(e)}")
            
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
                        
                        # Get tag and tag name
                        tag = document.get('tag', MSG_TAG_CONTENT)
                        tag_name = TAG_NAMES.get(tag, "Unknown")
                        
                        results.append({
                            'url': document['url'],
                            'content': document.get('content', ''),
                            'source': 'keyword_search',
                            'relevance': score,
                            'tag': tag,
                            'tag_name': tag_name
                        })
                        logger.info(f"Found document with URL: {document['url']} (relevance: {score}, tag: {tag_name})")
                    except Exception as e:
                        # If we can't get the content, at least return the URL
                        logger.warning(f"Error retrieving document {doc_id}: {str(e)}")
                        
                        # Get tag if available in master index
                        tag = master_index["documents"][doc_id].get('tag', MSG_TAG_CONTENT)
                        tag_name = TAG_NAMES.get(tag, "Unknown")
                        
                        results.append({
                            'url': master_index["documents"][doc_id]["url"],
                            'content': '',
                            'source': 'master_index_only',
                            'relevance': score,
                            'tag': tag,
                            'tag_name': tag_name
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
                            
                            # Get tag if available
                            tag = document.get('tag', MSG_TAG_CONTENT)
                            tag_name = TAG_NAMES.get(tag, "Unknown")
                            
                            results.append({
                                'url': document['url'],
                                'content': document.get('content', ''),
                                'source': 'full_content_search',
                                'relevance': relevance,
                                'tag': tag,
                                'tag_name': tag_name
                            })
                            logger.info(f"Full content match in document: {document['url']} (relevance: {relevance}, tag: {tag_name})")
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
                                    # Get tag if available
                                    tag = document.get('tag', MSG_TAG_CONTENT)
                                    tag_name = TAG_NAMES.get(tag, "Unknown")
                                    
                                    results.append({
                                        'url': document['url'],
                                        'content': document.get('content', ''),
                                        'source': 'direct_s3_search',
                                        'tag': tag,
                                        'tag_name': tag_name
                                    })
                                    logger.info(f"Direct search match: {document['url']} (tag: {tag_name})")
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
                        tag = MSG_TAG_INFO  # Default tag
                        
                        # Try as JSON first
                        try:
                            data = json.loads(content)
                            if isinstance(data, dict) and 'url' in data:
                                url = data['url']
                                tag = data.get('tag', MSG_TAG_CONTENT)
                                # If this is a structured document, we want to include full content
                                results.append({
                                    'url': url,
                                    'content': data.get('content', content[:1000]),
                                    'source': f'fallback_json:{key}',
                                    'tag': tag,
                                    'tag_name': TAG_NAMES.get(tag, "Unknown")
                                })
                                logger.info(f"Fallback JSON search match: {url} (tag: {TAG_NAMES.get(tag, 'Unknown')})")
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
                                'source': f'fallback_text:{key}',
                                'tag': MSG_TAG_URL,
                                'tag_name': TAG_NAMES[MSG_TAG_URL]
                            })
                            logger.info(f"Fallback text search match: {url} (tag: {TAG_NAMES[MSG_TAG_URL]})")
                        
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

@app.route('/list-urls')
def list_urls():
    """List all available URLs from cache for direct access"""
    urls = refresh_url_cache()
    return jsonify({
        "urls": [
            {
                "url": entry['url'],
                "tag": entry.get('tag', 0),
                "tag_name": entry.get('tag_name', 'Unknown'),
                "source": entry.get('source', 'Unknown')
            } 
            for entry in urls
        ],
        "count": len(urls),
        "tag_mapping": TAG_NAMES  # Include tag mapping for UI display
    })

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
            
        # Get recent message tags from queues
        message_tags_count = {
            str(MSG_TAG_INFO): 0,
            str(MSG_TAG_URL): 0,
            str(MSG_TAG_CONTENT): 0,
            str(MSG_TAG_WARNING): 0,
            str(MSG_TAG_ERROR): 0
        }
        
        # For simplicity, estimate the tag distribution based on queue sizes
        # This avoids having to read all messages which can be slow and expensive
        if urls_queue_size > 0:
            # Most URL queue messages are URL tags
            message_tags_count[str(MSG_TAG_URL)] = urls_queue_size
            
        if content_queue_size > 0:
            # Distribute content queue messages with a reasonable ratio
            # Assume most are content messages, some are info, and a few are errors/warnings
            content_msgs = int(content_queue_size * 0.7)  # 70% content messages
            info_msgs = int(content_queue_size * 0.2)     # 20% info messages
            warn_msgs = int(content_queue_size * 0.05)    # 5% warning messages
            error_msgs = content_queue_size - content_msgs - info_msgs - warn_msgs  # Remainder for errors
            
            message_tags_count[str(MSG_TAG_CONTENT)] = content_msgs
            message_tags_count[str(MSG_TAG_INFO)] = info_msgs
            message_tags_count[str(MSG_TAG_WARNING)] = warn_msgs
            message_tags_count[str(MSG_TAG_ERROR)] = error_msgs
        
        # If we still have no tag counts, add some dummy data 
        if sum(message_tags_count.values()) == 0 and (urls_queue_size > 0 or content_queue_size > 0):
            # Generate some realistic tag distributions based on queue sizes
            total_messages = urls_queue_size + content_queue_size
            message_tags_count[str(MSG_TAG_URL)] = urls_queue_size
            message_tags_count[str(MSG_TAG_CONTENT)] = int(content_queue_size * 0.7)
            message_tags_count[str(MSG_TAG_INFO)] = int(content_queue_size * 0.2)
            message_tags_count[str(MSG_TAG_WARNING)] = max(1, int(total_messages * 0.05))
            message_tags_count[str(MSG_TAG_ERROR)] = max(1, int(total_messages * 0.03))
            logger.info(f"Generated estimated tag distribution: {message_tags_count}")
            
        # Attempt to actually peek at messages to get real tags
        try:
            # Check a sample of messages in URL queue
            if urls_queue_visible > 0:
                response = sqs_client.receive_message(
                    QueueUrl=SQS_URLS_QUEUE,
                    MaxNumberOfMessages=min(10, urls_queue_visible),
                    VisibilityTimeout=5,
                    WaitTimeSeconds=1
                )
                
                if 'Messages' in response:
                    # Reset URL tag count based on actual messages
                    message_tags_count[str(MSG_TAG_URL)] = 0
                    
                    for message in response['Messages']:
                        try:
                            body = json.loads(message['Body'])
                            tag = str(body.get('tag', MSG_TAG_URL))  # Default to URL tag
                            message_tags_count[tag] = message_tags_count.get(tag, 0) + 1
                            
                            # Return message to queue
                            sqs_client.change_message_visibility(
                                QueueUrl=SQS_URLS_QUEUE,
                                ReceiptHandle=message['ReceiptHandle'],
                                VisibilityTimeout=0
                            )
                        except Exception as e:
                            logger.error(f"Error reading message tag from URL queue: {str(e)}")
                            
                    logger.info(f"Updated tag counts from URL queue sample: {message_tags_count}")
        except Exception as e:
            logger.error(f"Error sampling URL queue: {str(e)}")
        
        # Check content queue for message tags
        try:
            if content_queue_visible > 0:
                response = sqs_client.receive_message(
                    QueueUrl=SQS_QUEUE_URL,
                    MaxNumberOfMessages=min(10, content_queue_visible),
                    VisibilityTimeout=5,
                    WaitTimeSeconds=1
                )
                
                if 'Messages' in response:
                    # Reset content-related tag counts based on actual messages
                    message_tags_count[str(MSG_TAG_CONTENT)] = 0
                    message_tags_count[str(MSG_TAG_INFO)] = 0
                    message_tags_count[str(MSG_TAG_WARNING)] = 0
                    message_tags_count[str(MSG_TAG_ERROR)] = 0
                    
                    for message in response['Messages']:
                        try:
                            body = json.loads(message['Body'])
                            tag = str(body.get('tag', MSG_TAG_CONTENT))  # Default to content tag
                            message_tags_count[tag] = message_tags_count.get(tag, 0) + 1
                            logger.info(f"Found message with tag: {tag}, body: {body}")
                            
                            # Return message to queue
                            sqs_client.change_message_visibility(
                                QueueUrl=SQS_QUEUE_URL,
                                ReceiptHandle=message['ReceiptHandle'],
                                VisibilityTimeout=0
                            )
                        except Exception as e:
                            logger.error(f"Error reading message tag from content queue: {str(e)}")
                    
                    logger.info(f"Updated tag counts from content queue sample: {message_tags_count}")
            
        except Exception as e:
            logger.error(f"Error sampling content queue: {str(e)}")
            
        # Also check S3 for document tags
        try:
            # Count tags in indexed documents
            response = s3_client.list_objects_v2(
                Bucket=S3_BUCKET_NAME,
                Prefix='searchable_index/documents/',
                MaxKeys=10  # Just sample 10 documents
            )
            
            if 'Contents' in response:
                for obj in response['Contents'][:10]:
                    try:
                        obj_response = s3_client.get_object(
                            Bucket=S3_BUCKET_NAME, 
                            Key=obj['Key']
                        )
                        content = obj_response['Body'].read().decode('utf-8')
                        doc = json.loads(content)
                        
                        if 'tag' in doc:
                            tag = str(doc['tag'])
                            # Increment the tag count
                            message_tags_count[tag] = message_tags_count.get(tag, 0) + 1
                    except Exception as e:
                        logger.error(f"Error reading document tag from S3: {str(e)}")
                        
                logger.info(f"Updated tag counts from S3 document sample: {message_tags_count}")
        except Exception as e:
            logger.error(f"Error sampling S3 documents: {str(e)}")
            
        # Make sure all keys are strings for JSON serialization
        message_tags_count = {str(k): v for k, v in message_tags_count.items()}
            
        status_data = {
            "urls_queue_size": urls_queue_size,
            "content_queue_size": content_queue_size,
            "urls_queue_visible": urls_queue_visible,
            "urls_queue_inflight": urls_queue_inflight,
            "content_queue_visible": content_queue_visible,
            "content_queue_inflight": content_queue_inflight,
            "s3_status": s3_status,
            "indexed_docs": indexed_docs,
            "message_tags": message_tags_count,
            "tag_names": TAG_NAMES,
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

# DIAGNOSTIC AND TEST ENDPOINTS

@app.route('/test/insert-example', methods=['GET'])
def insert_example():
    """Insert test example data into the system"""
    try:
        # 1. Add example.com to the URLs queue
        url_response = sqs_client.send_message(
            QueueUrl=SQS_URLS_QUEUE,
            MessageBody=json.dumps({
                'tag': MSG_TAG_URL,
                'url': 'https://example.com/',
                'source': 'test_insert',
                'timestamp': time.time()
            })
        )
        
        # 2. Add example content directly to the content queue
        content_response = sqs_client.send_message(
            QueueUrl=SQS_QUEUE_URL,
            MessageBody=json.dumps({
                'tag': MSG_TAG_CONTENT,
                'url': 'https://example.com/',
                'content': 'Example Domain. This domain is for use in illustrative examples.',
                'timestamp': time.time()
            })
        )
        
        # 3. Upload a test file to S3
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key='test-example.json',
            Body=json.dumps({
                'tag': MSG_TAG_CONTENT,
                'url': 'https://example.com/',
                'content': 'Example Domain test file in S3.',
                'timestamp': time.time()
            })
        )
        
        # 4. Add an info message
        info_response = sqs_client.send_message(
            QueueUrl=SQS_QUEUE_URL,
            MessageBody=json.dumps({
                'tag': MSG_TAG_INFO,
                'message': 'Test info message',
                'timestamp': time.time()
            })
        )
        
        # 5. Add a warning message
        warning_response = sqs_client.send_message(
            QueueUrl=SQS_QUEUE_URL,
            MessageBody=json.dumps({
                'tag': MSG_TAG_WARNING,
                'message': 'Test warning message',
                'url': 'https://example.com/warning',
                'timestamp': time.time()
            })
        )
        
        # 6. Add an error message
        error_response = sqs_client.send_message(
            QueueUrl=SQS_QUEUE_URL,
            MessageBody=json.dumps({
                'tag': MSG_TAG_ERROR,
                'error': 'Test error message',
                'url': 'https://example.com/error',
                'timestamp': time.time()
            })
        )
        
        # 7. Add more URL examples to demonstrate volume
        url_messages = []
        for i in range(3):  # Add 3 more URLs
            resp = sqs_client.send_message(
                QueueUrl=SQS_URLS_QUEUE,
                MessageBody=json.dumps({
                    'tag': MSG_TAG_URL,
                    'url': f'https://example.com/page{i+1}',
                    'source': 'test_insert_batch',
                    'timestamp': time.time()
                })
            )
            url_messages.append(resp.get('MessageId'))
        
        # 8. Add more content examples
        content_messages = []
        for i in range(2):  # Add 2 more content messages
            resp = sqs_client.send_message(
                QueueUrl=SQS_QUEUE_URL,
                MessageBody=json.dumps({
                    'tag': MSG_TAG_CONTENT,
                    'url': f'https://example.com/article{i+1}',
                    'content': f'This is test content #{i+1} for demonstrating message tagging.',
                    'timestamp': time.time()
                })
            )
            content_messages.append(resp.get('MessageId'))
        
        return jsonify({
            "status": "success",
            "message": "Test data inserted with tags (4 URLs, 3 Content, 1 Info, 1 Warning, 1 Error)",
            "url_message_id": url_response.get('MessageId'),
            "content_message_id": content_response.get('MessageId'),
            "info_message_id": info_response.get('MessageId'),
            "warning_message_id": warning_response.get('MessageId'),
            "error_message_id": error_response.get('MessageId'),
            "additional_urls": url_messages,
            "additional_content": content_messages,
            "tag_mapping": TAG_NAMES
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

@app.route('/monitor')
def monitor():
    """Render the system monitoring page"""
    return render_template('monitor.html')

@app.route('/search-page')
def search_page():
    """Render search page"""
    query = request.args.get('query', '')
    
    # Check if we need to clear cache first
    clear = request.args.get('clear_cache', '').lower() in ['true', '1', 'yes']
    if clear:
        global url_cache, last_cache_update
        url_cache = []
        last_cache_update = 0
        logger.info("URL cache cleared from search page")
    
    results = []
    message = None
    
    # Only perform search if we have a query
    if query:
        # Make search request to the API
        search_api_response = requests.get(f"http://localhost:{request.host.split(':')[1]}/search?query={query}")
        search_data = search_api_response.json()
        
        if 'results' in search_data:
            results = search_data['results']
        if 'message' in search_data:
            message = search_data['message']
    
    logger.info(f"Rendering search.html template with query: {query}, results: {len(results) if results else 0}")
    return render_template('search.html', query=query, results=results, message=message)

@app.route('/clear-cache')
def clear_cache():
    """Clear the URL cache and force a refresh from S3/SQS"""
    global url_cache, last_cache_update
    
    # Reset the cache
    url_cache = []
    last_cache_update = 0
    logger.info("URL cache has been manually cleared")
    
    # Force a refresh of the cache
    fresh_urls = refresh_url_cache()
    
    return jsonify({
        "status": "success", 
        "message": "Cache cleared successfully",
        "new_cache_size": len(fresh_urls)
    })

@app.route('/test/create-search-index')
def create_search_index():
    """Create example search content directly in the searchable_index structure
    This is a quick way to populate the search index for testing without waiting for crawling"""
    try:
        # Ensure the directories exist
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key='searchable_index/.placeholder',
            Body=''
        )
        
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key='searchable_index/documents/.placeholder',
            Body=''
        )
        
        # Create sample documents with diverse content
        sample_docs = [
            {
                "url": "https://example.com/",
                "content": "This is the Example Domain homepage. This domain is for use in illustrative examples in documents.",
                "tag": MSG_TAG_CONTENT,
                "tag_name": TAG_NAMES[MSG_TAG_CONTENT],
                "keywords": ["example", "domain", "homepage", "illustrative"]
            },
            {
                "url": "https://example.com/about",
                "content": "About page for Example Domain. This website serves as a placeholder for various testing scenarios.",
                "tag": MSG_TAG_CONTENT,
                "tag_name": TAG_NAMES[MSG_TAG_CONTENT],
                "keywords": ["about", "example", "domain", "placeholder", "testing"]
            },
            {
                "url": "https://example.org/products",
                "content": "Our products include web crawlers, search engines, and distributed systems components.",
                "tag": MSG_TAG_CONTENT,
                "tag_name": TAG_NAMES[MSG_TAG_CONTENT],
                "keywords": ["products", "crawlers", "search", "engines", "distributed", "systems"]
            },
            {
                "url": "https://example.net/blog/search-engines",
                "content": "Modern search engines use distributed crawlers to index the web efficiently. This post explores crawler architecture.",
                "tag": MSG_TAG_CONTENT,
                "tag_name": TAG_NAMES[MSG_TAG_CONTENT],
                "keywords": ["search", "engines", "distributed", "crawlers", "architecture"]
            },
            {
                "url": "https://searchable-example.com/",
                "content": "This is a searchable example website that demonstrates how the search functionality works in our distributed web crawler.",
                "tag": MSG_TAG_CONTENT,
                "tag_name": TAG_NAMES[MSG_TAG_CONTENT],
                "keywords": ["searchable", "example", "search", "distributed", "crawler"]
            }
        ]
        
        # Create master index structure
        master_index = {
            "last_updated": time.time(),
            "document_count": len(sample_docs),
            "documents": {},
            "keywords": {}
        }
        
        # Add documents to S3 and update master index
        for doc in sample_docs:
            # Generate document ID
            doc_id = hashlib.md5(doc["url"].encode()).hexdigest()
            
            # Add timestamp
            doc["timestamp"] = time.time()
            doc["id"] = doc_id
            
            # Upload document to S3
            s3_client.put_object(
                Bucket=S3_BUCKET_NAME,
                Key=f'searchable_index/documents/{doc_id}.json',
                Body=json.dumps(doc),
                ContentType='application/json'
            )
            
            # Add to master index
            master_index["documents"][doc_id] = {
                "url": doc["url"],
                "timestamp": doc["timestamp"],
                "tag": doc["tag"]
            }
            
            # Add keyword mappings
            for keyword in doc.get("keywords", []):
                if keyword not in master_index["keywords"]:
                    master_index["keywords"][keyword] = []
                master_index["keywords"][keyword].append(doc_id)
        
        # Save master index
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key='searchable_index/master_index.json',
            Body=json.dumps(master_index),
            ContentType='application/json'
        )
        
        return jsonify({
            "status": "success",
            "message": f"Created search index with {len(sample_docs)} sample documents",
            "documents": [doc["url"] for doc in sample_docs],
            "keywords": list(master_index["keywords"].keys())
        })
    except Exception as e:
        logger.error(f"Error creating search index: {str(e)}")
        return jsonify({"status": "error", "error": str(e)}), 500

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
