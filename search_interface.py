import os
import json
import logging
import re
from flask import Flask, render_template, request, jsonify
from dotenv import load_dotenv
import boto3
from whoosh.index import open_dir
from whoosh.qparser import QueryParser
from whoosh.highlight import Formatter, Highlighter, ContextFragmenter
import hashlib

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('SearchInterface')

# AWS Configuration
AWS_REGION = os.getenv('AWS_REGION', 'eu-north-1')
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME', 'web-crawler-content')
s3_client = boto3.client('s3', region_name=AWS_REGION)

app = Flask(__name__)

class HTMLFormatter(Formatter):
    """Custom formatter for highlighting search terms in HTML"""
    def format_token(self, text, token, replace=False):
        return f"<span class='highlight'>{text}</span>"

def get_index():
    """Get or create the Whoosh index"""
    index_dir = "index"
    if not os.path.exists(index_dir):
        logger.error("Index directory does not exist. Run the indexer first.")
        return None
    
    try:
        return open_dir(index_dir)
    except Exception as e:
        logger.error(f"Error opening index: {str(e)}")
        return None

def get_document_from_s3(url_hash):
    """Retrieve document content from S3 by URL hash"""
    try:
        # Get metadata first
        metadata_key = f"metadata/{url_hash}.json"
        response = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=metadata_key)
        metadata = json.loads(response['Body'].read().decode('utf-8'))
        
        # Get text content
        text_key = metadata.get('text_key')
        if not text_key:
            logger.warning(f"No text key found in metadata for {url_hash}")
            return {"url": metadata.get('url'), "content": "No content available"}
            
        text_response = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=text_key)
        text_content = text_response['Body'].read().decode('utf-8')
        
        # Get HTML content (for potential future use)
        html_key = metadata.get('html_key')
        html_content = None
        if html_key:
            try:
                html_response = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=html_key)
                html_content = html_response['Body'].read().decode('utf-8')
            except Exception as e:
                logger.warning(f"Could not retrieve HTML content: {str(e)}")
        
        return {
            "url": metadata.get('url'),
            "content": text_content,
            "html": html_content,
            "timestamp": metadata.get('timestamp')
        }
    except Exception as e:
        logger.error(f"Error retrieving document from S3: {str(e)}")
        return {"url": "unknown", "content": "Error retrieving content"}

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

@app.route('/')
def home():
    """Render the search page"""
    return render_template('index.html')

@app.route('/search')
def search():
    """Handle search requests"""
    query_text = request.args.get('q', '')
    if not query_text:
        return jsonify({"results": [], "message": "No query provided"})
    
    index = get_index()
    if not index:
        return jsonify({"results": [], "message": "Search index is not available"})
    
    try:
        # Extract query terms for highlighting
        query_terms = [term.strip() for term in query_text.split() if len(term.strip()) > 2]
        
        # Search the index
        with index.searcher() as searcher:
            query = QueryParser("content", index.schema).parse(query_text)
            results = searcher.search(query, limit=10)
            
            # Setup highlighter
            highlighter = Highlighter(fragmenter=ContextFragmenter(maxchars=100, surround=50),
                                      formatter=HTMLFormatter())
            
            # Format results
            formatted_results = []
            for hit in results:
                url = hit['url']
                # Extract URL hash from URL for S3 lookup
                url_hash = hashlib.md5(url.encode()).hexdigest()
                
                # Get full document from S3
                document = get_document_from_s3(url_hash)
                full_content = document.get("content", "")
                
                # Generate title from URL
                parts = url.split('://')[-1].split('/')
                domain = parts[0]
                path = '/'.join(parts[1:]) if len(parts) > 1 else ""
                title = path.replace('-', ' ').replace('_', ' ').title() if path else domain
                
                # Generate snippet with highlighted terms
                snippet = generate_snippet(full_content, query_terms)
                
                formatted_results.append({
                    "url": url,
                    "title": title,
                    "domain": domain,
                    "snippet": snippet,
                    "score": hit.score,
                    "timestamp": document.get("timestamp")
                })
            
            return jsonify({
                "results": formatted_results,
                "count": len(formatted_results),
                "query": query_text
            })
            
    except Exception as e:
        logger.error(f"Error searching for '{query_text}': {str(e)}")
        return jsonify({"results": [], "message": f"Error: {str(e)}"})

@app.route('/api/status')
def status():
    """Get system status information"""
    try:
        # Basic system information
        index = get_index()
        if index:
            with index.searcher() as searcher:
                doc_count = searcher.doc_count()
        else:
            doc_count = 0
            
        return jsonify({
            "status": "online",
            "indexed_documents": doc_count,
            "s3_bucket": S3_BUCKET_NAME
        })
    except Exception as e:
        logger.error(f"Error getting system status: {str(e)}")
        return jsonify({"status": "error", "message": str(e)})

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000) 