import os
import json
import logging
import re
import time
import hashlib
import uuid
from flask import Flask, render_template, request, jsonify, redirect, url_for, flash
from dotenv import load_dotenv
import boto3
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from whoosh.index import create_in, open_dir
from whoosh.fields import Schema, TEXT, ID
from whoosh.qparser import QueryParser
from whoosh.highlight import Formatter, Highlighter, ContextFragmenter
import traceback

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('StandaloneWebApp')

# AWS Configuration
AWS_REGION = os.getenv('AWS_REGION', 'eu-north-1')
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME', 'web-crawler-content')
USE_LOCAL_STORAGE = os.getenv('USE_LOCAL_STORAGE', 'false').lower() == 'true'

# Initialize AWS S3 client
try:
    s3_client = boto3.client('s3', region_name=AWS_REGION)
    logger.info(f"Initialized S3 client with region {AWS_REGION}")
except Exception as e:
    logger.error(f"Error initializing S3 client: {str(e)}")
    s3_client = None

# Local storage paths
LOCAL_STORAGE_DIR = os.path.join(os.getcwd(), 'local_storage')
HTML_DIR = os.path.join(LOCAL_STORAGE_DIR, 'html')
TEXT_DIR = os.path.join(LOCAL_STORAGE_DIR, 'text')
METADATA_DIR = os.path.join(LOCAL_STORAGE_DIR, 'metadata')

# Create local storage directories if they don't exist
for directory in [HTML_DIR, TEXT_DIR, METADATA_DIR]:
    if not os.path.exists(directory):
        os.makedirs(directory)
        logger.info(f"Created local storage directory: {directory}")

# Ensure S3 bucket exists
if s3_client and not USE_LOCAL_STORAGE:
    try:
        s3_client.head_bucket(Bucket=S3_BUCKET_NAME)
        logger.info(f"S3 bucket {S3_BUCKET_NAME} exists")
    except Exception as e:
        logger.info(f"Creating S3 bucket: {S3_BUCKET_NAME}")
        try:
            s3_client.create_bucket(
                Bucket=S3_BUCKET_NAME,
                CreateBucketConfiguration={'LocationConstraint': AWS_REGION}
            )
            logger.info(f"Created S3 bucket: {S3_BUCKET_NAME}")
        except Exception as e:
            logger.error(f"Error creating S3 bucket: {str(e)}")
            logger.info("Falling back to local storage")
            USE_LOCAL_STORAGE = True

# Create Flask app
app = Flask(__name__)
app.secret_key = os.getenv('FLASK_SECRET_KEY', os.urandom(24).hex())

# Create or open Whoosh index
def ensure_index_exists():
    index_dir = "index"
    if not os.path.exists(index_dir):
        os.makedirs(index_dir)
        
    schema = Schema(
        url=ID(stored=True),
        content=TEXT(stored=True)
    )
    
    if not os.listdir(index_dir):
        logger.info("Creating new Whoosh index")
        index = create_in(index_dir, schema)
    else:
        logger.info("Opening existing Whoosh index")
        index = open_dir(index_dir)
    
    return index

# Get index
def get_index():
    index_dir = "index"
    if not os.path.exists(index_dir):
        return ensure_index_exists()
    
    try:
        return open_dir(index_dir)
    except Exception as e:
        logger.error(f"Error opening index: {str(e)}")
        return ensure_index_exists()

# File storage operations
def store_local_file(content, directory, filename):
    """Store content to a local file"""
    filepath = os.path.join(directory, filename)
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        logger.info(f"Stored content in local file: {filepath}")
        return True
    except Exception as e:
        logger.error(f"Error storing local file: {str(e)}")
        return False

def read_local_file(directory, filename):
    """Read content from a local file"""
    filepath = os.path.join(directory, filename)
    try:
        if not os.path.exists(filepath):
            logger.error(f"File does not exist: {filepath}")
            return None
        
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        return content
    except Exception as e:
        logger.error(f"Error reading local file {filepath}: {str(e)}")
        return None

# Crawler functionality
class Crawler:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        self.session.timeout = 10
        self.session.verify = False  # Disable SSL verification
        self.index = get_index()
    
    def fetch_page(self, url):
        """Fetch web page content"""
        try:
            logger.info(f"Fetching page: {url}")
            response = self.session.get(url)
            response.raise_for_status()
            return response.text
        except Exception as e:
            logger.error(f"Error fetching page {url}: {str(e)}")
            return None
    
    def extract_text(self, html):
        """Extract text content from HTML"""
        if not html:
            return ""
        
        try:
            soup = BeautifulSoup(html, 'html.parser')
            # Remove script and style elements
            for element in soup(["script", "style", "nav", "header", "footer"]):
                element.decompose()
            
            text = soup.get_text(separator=' ', strip=True)
            # Basic text cleaning
            text = ' '.join(text.split())
            return text
        except Exception as e:
            logger.error(f"Error extracting text: {str(e)}")
            return ""
    
    def extract_urls(self, html, base_url):
        """Extract URLs from HTML"""
        if not html:
            return []
        
        try:
            soup = BeautifulSoup(html, 'html.parser')
            urls = []
            
            for link in soup.find_all('a', href=True):
                href = link['href']
                absolute_url = urljoin(base_url, href)
                if self.is_valid_url(absolute_url):
                    urls.append(absolute_url)
            
            return urls[:10]  # Limit to 10 URLs per page
        except Exception as e:
            logger.error(f"Error extracting URLs: {str(e)}")
            return []
    
    def is_valid_url(self, url):
        """Check if URL is valid"""
        try:
            parsed = urlparse(url)
            return all([parsed.scheme, parsed.netloc]) and parsed.scheme in ['http', 'https']
        except Exception:
            return False
    
    def store_content(self, url, html, text):
        """Store content in S3 or locally"""
        if not html:
            logger.warning(f"No HTML content to store for {url}")
            return None
        
        url_hash = hashlib.md5(url.encode()).hexdigest()
        
        if USE_LOCAL_STORAGE or not s3_client:
            # Store locally
            html_filename = f"{url_hash}.html"
            text_filename = f"{url_hash}.txt"
            metadata_filename = f"{url_hash}.json"
            
            # Store HTML and text
            html_stored = store_local_file(html, HTML_DIR, html_filename)
            text_stored = store_local_file(text, TEXT_DIR, text_filename)
            
            # Store metadata
            metadata = {
                'url': url,
                'timestamp': time.time(),
                'html_filename': html_filename,
                'text_filename': text_filename
            }
            
            metadata_stored = store_local_file(json.dumps(metadata), METADATA_DIR, metadata_filename)
            
            if html_stored and text_stored and metadata_stored:
                logger.info(f"Content stored locally for {url}")
                return url_hash
            else:
                logger.error(f"Failed to store content locally for {url}")
                return None
        else:
            # Store in S3
            try:
                html_key = f"html/{url_hash}.html"
                text_key = f"text/{url_hash}.txt"
                metadata_key = f"metadata/{url_hash}.json"
                
                # Store HTML
                s3_client.put_object(
                    Bucket=S3_BUCKET_NAME,
                    Key=html_key,
                    Body=html,
                    ContentType='text/html'
                )
                
                # Store text
                s3_client.put_object(
                    Bucket=S3_BUCKET_NAME,
                    Key=text_key,
                    Body=text,
                    ContentType='text/plain'
                )
                
                # Also store locally as backup
                store_local_file(html, HTML_DIR, f"{url_hash}.html")
                store_local_file(text, TEXT_DIR, f"{url_hash}.txt")
                
                # Store metadata
                metadata = {
                    'url': url,
                    'timestamp': time.time(),
                    'html_key': html_key,
                    'text_key': text_key
                }
                
                s3_client.put_object(
                    Bucket=S3_BUCKET_NAME,
                    Key=metadata_key,
                    Body=json.dumps(metadata),
                    ContentType='application/json'
                )
                
                # Store metadata locally too
                store_local_file(json.dumps(metadata), METADATA_DIR, f"{url_hash}.json")
                
                logger.info(f"Content stored in S3 for {url}")
                return url_hash
            except Exception as e:
                logger.error(f"Error storing content in S3: {str(e)}")
                logger.error(traceback.format_exc())
                
                # Fall back to local storage
                return self.store_content(url, html, text)
    
    def index_content(self, url, text):
        """Index content using Whoosh"""
        try:
            writer = self.index.writer()
            writer.add_document(url=url, content=text)
            writer.commit()
            logger.info(f"Indexed content for {url}")
            return True
        except Exception as e:
            logger.error(f"Error indexing content: {str(e)}")
            try:
                writer.cancel()
            except:
                pass
            return False
    
    def crawl_url(self, url):
        """Crawl a single URL"""
        html = self.fetch_page(url)
        if not html:
            return {
                'success': False,
                'message': f"Failed to fetch {url}"
            }
        
        text = self.extract_text(html)
        urls = self.extract_urls(html, url)
        
        # Store content in S3 or locally
        url_hash = self.store_content(url, html, text)
        
        # Index content
        indexed = self.index_content(url, text)
        
        return {
            'success': True,
            'url': url,
            'text_length': len(text),
            'urls_found': len(urls),
            'stored': url_hash is not None,
            'indexed': indexed,
            'extracted_urls': urls,
            'storage_type': 'local' if USE_LOCAL_STORAGE or not s3_client else 's3'
        }

# Content retrieval functions
def get_document_content(url_hash):
    """Get document content from S3 or local storage"""
    # First try to get metadata
    metadata = None
    
    # Try to get metadata from local storage first
    local_metadata_path = os.path.join(METADATA_DIR, f"{url_hash}.json")
    if os.path.exists(local_metadata_path):
        try:
            with open(local_metadata_path, 'r', encoding='utf-8') as f:
                metadata = json.loads(f.read())
            logger.info(f"Got metadata from local storage for {url_hash}")
        except Exception as e:
            logger.error(f"Error reading local metadata: {str(e)}")
    
    # If local metadata not found, try S3
    if metadata is None and s3_client and not USE_LOCAL_STORAGE:
        try:
            metadata_key = f"metadata/{url_hash}.json"
            response = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=metadata_key)
            metadata = json.loads(response['Body'].read().decode('utf-8'))
            logger.info(f"Got metadata from S3 for {url_hash}")
            
            # Store metadata locally for future use
            with open(local_metadata_path, 'w', encoding='utf-8') as f:
                f.write(json.dumps(metadata))
        except Exception as e:
            logger.error(f"Error getting metadata from S3: {str(e)}")
    
    if metadata is None:
        logger.error(f"No metadata found for {url_hash}")
        return {
            "url": "unknown",
            "content": "No metadata found for this URL",
            "error": "metadata_not_found"
        }
    
    # Now get the content
    content = None
    
    # First check if we have local content
    local_text_path = os.path.join(TEXT_DIR, f"{url_hash}.txt")
    if os.path.exists(local_text_path):
        try:
            with open(local_text_path, 'r', encoding='utf-8') as f:
                content = f.read()
            logger.info(f"Got content from local storage for {url_hash}")
        except Exception as e:
            logger.error(f"Error reading local content: {str(e)}")
    
    # If local content not found, try S3
    if content is None and s3_client and not USE_LOCAL_STORAGE:
        try:
            text_key = metadata.get('text_key')
            if text_key:
                response = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=text_key)
                content = response['Body'].read().decode('utf-8')
                logger.info(f"Got content from S3 for {url_hash}")
                
                # Store content locally for future use
                with open(local_text_path, 'w', encoding='utf-8') as f:
                    f.write(content)
            else:
                logger.error(f"No text_key in metadata for {url_hash}")
        except Exception as e:
            logger.error(f"Error getting content from S3: {str(e)}")
    
    if content is None:
        logger.error(f"No content found for {url_hash}")
        return {
            "url": metadata.get('url', 'unknown'),
            "content": "Error retrieving content",
            "error": "content_not_found",
            "timestamp": metadata.get('timestamp')
        }
    
    # Return the document
    return {
        "url": metadata.get('url'),
        "content": content,
        "timestamp": metadata.get('timestamp')
    }

def generate_snippet(content, query_terms, max_length=200):
    """Generate a relevant snippet from content based on query terms"""
    if not content or not query_terms:
        return content[:max_length] + "..." if len(content) > max_length else content
    
    # Convert query terms to regex pattern for case-insensitive search
    pattern = '|'.join(re.escape(term) for term in query_terms)
    matches = list(re.finditer(pattern, content, re.IGNORECASE))
    
    if not matches:
        return content[:max_length] + "..." if len(content) > max_length else content
    
    # Find the best region with most matches
    best_pos = matches[0].start()
    
    # Extract snippet around the best position
    start = max(0, best_pos - max_length // 2)
    end = min(len(content), start + max_length)
    
    # Adjust start to avoid cutting words
    if start > 0:
        while start > 0 and content[start] != ' ':
            start -= 1
    
    # Adjust end to avoid cutting words
    if end < len(content):
        while end < len(content) and content[end] != ' ':
            end += 1
    
    snippet = content[start:end]
    
    # Add ellipsis if needed
    if start > 0:
        snippet = "..." + snippet
    if end < len(content):
        snippet = snippet + "..."
    
    # Highlight query terms
    for term in query_terms:
        pattern = re.compile(re.escape(term), re.IGNORECASE)
        snippet = pattern.sub(r'<span class="highlight">\g<0></span>', snippet)
    
    return snippet

# Flask routes
@app.route('/')
def home():
    """Render the home page with search and crawl forms"""
    return render_template('home.html')

@app.route('/search')
def search():
    """Handle search requests"""
    query_text = request.args.get('q', '')
    if not query_text:
        return jsonify({"results": [], "message": "No query provided"})
    
    index = get_index()
    
    try:
        # Extract query terms for highlighting
        query_terms = [term.strip() for term in query_text.split() if len(term.strip()) > 2]
        
        # Search the index
        with index.searcher() as searcher:
            query = QueryParser("content", index.schema).parse(query_text)
            results = searcher.search(query, limit=10)
            
            # Format results
            formatted_results = []
            for hit in results:
                url = hit['url']
                url_hash = hashlib.md5(url.encode()).hexdigest()
                
                # Get document content
                document = get_document_content(url_hash)
                full_content = document.get("content", "")
                
                # Generate title from URL
                parts = url.split('://')[-1].split('/')
                domain = parts[0]
                path = '/'.join(parts[1:]) if len(parts) > 1 else ""
                title = path.replace('-', ' ').replace('_', ' ').title() if path else domain
                
                # Generate snippet
                snippet = generate_snippet(full_content, query_terms)
                
                formatted_results.append({
                    "url": url,
                    "title": title,
                    "domain": domain,
                    "snippet": snippet,
                    "score": hit.score,
                    "timestamp": document.get("timestamp"),
                    "error": document.get("error")
                })
            
            return jsonify({
                "results": formatted_results,
                "count": len(formatted_results),
                "query": query_text
            })
    except Exception as e:
        logger.error(f"Error searching: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({"results": [], "message": f"Error: {str(e)}"})

@app.route('/crawl', methods=['POST'])
def crawl():
    """Handle URL crawl requests"""
    url = request.form.get('url')
    if not url:
        return jsonify({"success": False, "message": "No URL provided"})
    
    crawler = Crawler()
    if not crawler.is_valid_url(url):
        return jsonify({"success": False, "message": "Invalid URL format"})
    
    result = crawler.crawl_url(url)
    return jsonify(result)

@app.route('/api/status')
def status():
    """Get system status information"""
    try:
        # Basic system information
        index = get_index()
        with index.searcher() as searcher:
            doc_count = searcher.doc_count()
        
        storage_type = "local" if USE_LOCAL_STORAGE or not s3_client else "s3"
        
        return jsonify({
            "status": "online",
            "indexed_documents": doc_count,
            "storage_type": storage_type,
            "s3_bucket": S3_BUCKET_NAME if not USE_LOCAL_STORAGE and s3_client else None,
            "use_local_storage": USE_LOCAL_STORAGE,
            "s3_available": s3_client is not None
        })
    except Exception as e:
        logger.error(f"Error getting system status: {str(e)}")
        return jsonify({"status": "error", "message": str(e)})

@app.route('/debug/content/<url_hash>')
def debug_content(url_hash):
    """Debug endpoint to directly view content by URL hash"""
    document = get_document_content(url_hash)
    return jsonify(document)

@app.route('/fix-content-error', methods=['POST'])
def fix_content_error():
    """Attempt to fix content retrieval errors by re-crawling the URL"""
    url = request.form.get('url')
    if not url:
        return jsonify({"success": False, "message": "No URL provided"})
    
    logger.info(f"Attempting to fix content for URL: {url}")
    crawler = Crawler()
    result = crawler.crawl_url(url)
    
    return jsonify({
        "success": result['success'],
        "message": "Re-crawled URL to fix content error" if result['success'] else "Failed to re-crawl URL",
        "details": result
    })

@app.route('/storage-settings', methods=['POST'])
def storage_settings():
    """Update storage settings"""
    global USE_LOCAL_STORAGE
    
    use_local = request.form.get('use_local_storage', 'false').lower() == 'true'
    USE_LOCAL_STORAGE = use_local
    
    return jsonify({
        "success": True,
        "message": f"Storage settings updated. Using {'local' if USE_LOCAL_STORAGE else 'S3'} storage."
    })

if __name__ == '__main__':
    # Ensure index exists
    ensure_index_exists()
    
    # Run Flask app
    app.run(debug=True, host='0.0.0.0', port=5000) 