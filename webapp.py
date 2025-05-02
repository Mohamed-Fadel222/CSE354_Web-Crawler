from flask import Flask, render_template, request, redirect, url_for, jsonify
import boto3, json, os
from dotenv import load_dotenv
from whoosh.index import open_dir
from whoosh.qparser import QueryParser
import datetime
import tempfile
import tarfile
import shutil

# ─── Config ───────────────────────────────────────────────────────────
load_dotenv()
AWS_REGION    = os.getenv('AWS_REGION')
SQS_URLS_QUEUE = os.getenv('SQS_URLS_QUEUE')
S3_BUCKET     = os.getenv('S3_BUCKET')
INDEX_KEY     = 'search_index/whoosh_index.tar.gz'  # S3 key for the index

# ─── AWS Clients ──────────────────────────────────────────────────────
sqs = boto3.client('sqs', region_name=AWS_REGION)
s3 = boto3.client('s3', region_name=AWS_REGION)

# ─── S3-based Whoosh wrapper for search ─────────────────────────────────
class S3Indexer:
    def __init__(self):
        self.index_dir = tempfile.mkdtemp()
        self._download_index()

    def _download_index(self):
        """Download the index from S3 and extract it"""
        try:
            local_tar_path = os.path.join(self.index_dir, 'index.tar.gz')
            s3.download_file(S3_BUCKET, INDEX_KEY, local_tar_path)
            
            # Extract the tar file
            with tarfile.open(local_tar_path, 'r:gz') as tar:
                tar.extractall(path=self.index_dir)
                
            # Remove the tar file after extraction
            os.remove(local_tar_path)
        except Exception as e:
            print(f"Error downloading index: {e}")
            # Create empty directory structure if no index exists
            os.makedirs(os.path.join(self.index_dir, 'MAIN'), exist_ok=True)
    
    def search(self, text):
        """Search the index"""
        try:
            if not os.path.exists(os.path.join(self.index_dir, 'MAIN')):
                return []
                
            index = open_dir(self.index_dir)
            with index.searcher() as searcher:
                query = QueryParser('content', index.schema).parse(text)
                results = searcher.search(query, limit=20)
                return [{'url': hit['url'], 'score': hit.score} for hit in results]
        except Exception as e:
            print(f"Search error: {e}")
            return []
    
    def cleanup(self):
        """Remove the temporary directory"""
        try:
            shutil.rmtree(self.index_dir)
        except Exception as e:
            print(f"Error cleaning up: {e}")

# Initialize indexer
indexer = S3Indexer()

# ─── Helper Functions ──────────────────────────────────────────────────
def get_queue_stats():
    """Get SQS queue statistics"""
    try:
        # Get more detailed queue attributes
        queue_attrs = sqs.get_queue_attributes(
            QueueUrl=SQS_URLS_QUEUE,
            AttributeNames=['All']
        )
        
        queue_size = int(queue_attrs['Attributes'].get('ApproximateNumberOfMessages', 0))
        in_progress = int(queue_attrs['Attributes'].get('ApproximateNumberOfMessagesNotVisible', 0))
        delayed = int(queue_attrs['Attributes'].get('ApproximateNumberOfMessagesDelayed', 0))
        
        return queue_size, in_progress, delayed
    except Exception as e:
        print(f"Error getting queue stats: {e}")
        return 0, 0, 0

def get_crawled_count():
    """Get count of crawled URLs from S3"""
    try:
        # List objects in the text folder to count crawled pages
        # Use pagination to handle large numbers of objects
        paginator = s3.get_paginator('list_objects_v2')
        total_count = 0
        
        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix='text/'):
            if 'Contents' in page:
                total_count += len(page['Contents'])
                
        return total_count
    except Exception as e:
        print(f"Error getting crawled count: {e}")
        return 0

# ─── Flask app ────────────────────────────────────────────────────────
app = Flask(__name__, static_folder='static')

@app.route('/', methods=['GET','POST'])
def index():
    sent = None
    if request.method == 'POST':
        # User pastes one URL per line
        urls_text = request.form.get('urls','')
        urls = [u.strip() for u in urls_text.splitlines() if u.strip()]
        for url in urls:
            sqs.send_message(
                QueueUrl=SQS_URLS_QUEUE,
                MessageBody=json.dumps({'url': url})
            )
        sent = len(urls)
    
    # Get queue stats and crawled count
    queue_size, in_progress, delayed = get_queue_stats()
    
    # Get crawled count (this might be slow for large buckets)
    # so we'll only do it for the main page load, not for API updates
    crawled_count = get_crawled_count()
    
    return render_template('index.html', 
                         sent=sent, 
                         queue_size=queue_size, 
                         in_progress=in_progress,
                         delayed=delayed,
                         crawled_count=crawled_count)

@app.route('/search', methods=['GET','POST'])
def search():
    results = []
    query = ''
    if request.method == 'POST':
        query = request.form.get('query','')
        # Refresh index from S3 before search to get latest data
        indexer._download_index()
        results = indexer.search(query)
    return render_template('search.html', query=query, results=results)

@app.route('/api/status', methods=['GET'])
def api_status():
    """API endpoint to get crawler status information"""
    try:
        # Get queue stats
        queue_size, in_progress, delayed = get_queue_stats()
        
        # For API calls, use a cached approach or count estimate for crawled count
        # to avoid slow S3 listing operations
        crawled_count = 0
        try:
            # Check if we have a head marker object with the count
            response = s3.get_object(
                Bucket=S3_BUCKET,
                Key='stats/crawled_count.json'
            )
            stats = json.loads(response['Body'].read().decode('utf-8'))
            crawled_count = stats.get('count', 0)
        except:
            # Fallback to a quick estimate by checking only the first 1000 objects
            try:
                response = s3.list_objects_v2(
                    Bucket=S3_BUCKET,
                    Prefix='text/',
                    MaxKeys=1000
                )
                if 'Contents' in response:
                    crawled_count = len(response['Contents'])
                    if response.get('IsTruncated', False):
                        # Indicate this is a partial count if there are more than 1000
                        crawled_count = str(crawled_count) + "+"
            except Exception as s3_error:
                print(f"Error getting crawled count from S3: {s3_error}")
        
        # Debug output
        print(f"API Status: Queue={queue_size}, In Progress={in_progress}, Delayed={delayed}, Crawled={crawled_count}")
        
        return jsonify({
            'queue_size': queue_size,
            'in_progress': in_progress,
            'delayed': delayed,
            'crawled_count': crawled_count,
            'timestamp': datetime.datetime.now().isoformat()
        })
    except Exception as e:
        print(f"API Status Error: {str(e)}")
        return jsonify({
            'error': str(e),
            'queue_size': 0,
            'in_progress': 0,
            'delayed': 0,
            'crawled_count': 0,
            'timestamp': datetime.datetime.now().isoformat()
        })

# Cleanup on exit
import atexit
@atexit.register
def cleanup():
    indexer.cleanup()

if __name__ == '__main__':
    # Runs on http://localhost:5000
    app.run(debug=True)
