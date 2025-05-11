from flask import Flask, render_template, request, redirect, url_for, jsonify
import boto3, json, os, time
from dotenv import load_dotenv
from whoosh.index import open_dir
from whoosh.qparser import QueryParser, MultifieldParser
from datetime import datetime
import requests

# ─── Config ───────────────────────────────────────────────────────────
load_dotenv()
AWS_REGION    = os.getenv('AWS_REGION')
SQS_URLS_QUEUE = os.getenv('SQS_URLS_QUEUE')
SQS_CONTENT_QUEUE = os.getenv('SQS_CONTENT_QUEUE')
S3_BUCKET     = os.getenv('S3_BUCKET')
MASTER_NODE_URL = os.getenv('MASTER_NODE_URL', 'http://localhost:5001')
CRAWLER_NODES = os.getenv('CRAWLER_NODES', '').split(',')
INDEXER_NODES = os.getenv('INDEXER_NODES', '').split(',')

# ─── AWS Clients ──────────────────────────────────────────────────────
sqs = boto3.client('sqs', region_name=AWS_REGION)
s3 = boto3.client('s3', region_name=AWS_REGION)

# ─── Enhanced Whoosh wrapper for search ───────────────────────────────
class EnhancedIndexer:
    def __init__(self, index_dir='index'):
        self.index = open_dir(index_dir)
        self.schema = self.index.schema
        self.last_health_check = time.time()
        self.stats = {
            "total_searches": 0,
            "start_time": datetime.now()
        }

    def search(self, text):
        self.stats["total_searches"] += 1
        
        with self.index.searcher() as searcher:
            # Search in both content and title fields
            parser = MultifieldParser(['content', 'title'], self.schema)
            query = parser.parse(text)
            results = searcher.search(query, limit=20)
            
            # Format results with more details
            formatted_results = []
            for hit in results:
                result = {
                    'url': hit['url'],
                    'title': hit.get('title', hit['url']),
                    'snippet': hit.highlights('content', top=1),
                    'last_indexed': hit.get('last_indexed', 'Unknown'),
                    'score': hit.score,
                    'message_type': hit.get('msg_type', 'CONTENT')
                }
                formatted_results.append(result)
            return formatted_results

    def get_stats(self):
        with self.index.searcher() as searcher:
            uptime = (datetime.now() - self.stats["start_time"]).total_seconds()
            searches_per_minute = 0
            if uptime > 0:
                searches_per_minute = (self.stats["total_searches"] / (uptime / 60))
                
            return {
                'total_documents': searcher.doc_count(),
                'total_searches': self.stats["total_searches"],
                'searches_per_minute': round(searches_per_minute, 2),
                'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'last_health_check': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(self.last_health_check))
            }
            
    def health_check(self):
        """Perform health check on the index"""
        self.last_health_check = time.time()
        try:
            with self.index.searcher() as searcher:
                doc_count = searcher.doc_count()
                return {
                    'status': 'healthy',
                    'document_count': doc_count,
                    'timestamp': datetime.now().isoformat()
                }
        except Exception as e:
            return {
                'status': 'unhealthy',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }

indexer = EnhancedIndexer()

# ─── Flask app ────────────────────────────────────────────────────────
app = Flask(__name__)

# Store system health information
system_health = {
    'status': 'initializing',
    'components': {
        'master': {'status': 'unknown', 'last_check': None},
        'crawler': {'status': 'unknown', 'last_check': None},
        'indexer': {'status': 'unknown', 'last_check': None},
        'webapp': {'status': 'healthy', 'last_check': datetime.now().isoformat()}
    },
    'last_check': datetime.now().isoformat()
}

@app.route('/', methods=['GET','POST'])
def index():
    sent = None
    error = None
    if request.method == 'POST':
        urls_text = request.form.get('urls','')
        urls = [u.strip() for u in urls_text.splitlines() if u.strip()]
        
        # Basic URL validation
        valid_urls = []
        for url in urls:
            if url.startswith(('http://', 'https://')):
                valid_urls.append(url)
            else:
                error = f"Invalid URL format: {url}. URLs must start with http:// or https://"
                break
        
        if not error and valid_urls:
            for url in valid_urls:
                try:
                    sqs.send_message(
                        QueueUrl=SQS_URLS_QUEUE,
                        MessageBody=json.dumps({
                            'url': url,
                            'message_type': 'URL_TO_CRAWL',
                            'source': 'webapp',
                            'timestamp': datetime.now().isoformat()
                        })
                    )
                except Exception as e:
                    error = f"Error sending URL to queue: {str(e)}"
                    break
            if not error:
                sent = len(valid_urls)
    
    # Get queue statistics
    try:
        queue_attrs = sqs.get_queue_attributes(
            QueueUrl=SQS_URLS_QUEUE,
            AttributeNames=['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible']
        )
        queue_stats = {
            'pending': queue_attrs['Attributes']['ApproximateNumberOfMessages'],
            'processing': queue_attrs['Attributes']['ApproximateNumberOfMessagesNotVisible']
        }
    except Exception as e:
        queue_stats = {'pending': 'N/A', 'processing': 'N/A'}
    
    return render_template('index.html', sent=sent, error=error, queue_stats=queue_stats)

@app.route('/search', methods=['GET','POST'])
def search():
    results = []
    query = ''
    stats = None
    
    if request.method == 'POST':
        query = request.form.get('query','')
        if query:
            results = indexer.search(query)
            stats = indexer.get_stats()
    
    return render_template('search.html', query=query, results=results, stats=stats)

@app.route('/monitor')
def monitor():
    try:
        # Get status from master node
        try:
            master_response = requests.get(f"{MASTER_NODE_URL}/status", timeout=5)
            if master_response.status_code == 200:
                master_status = master_response.json()
                system_health['components']['master'] = {
                    'status': 'healthy',
                    'last_check': datetime.now().isoformat()
                }
            else:
                master_status = {'error': f"Status code: {master_response.status_code}"}
                system_health['components']['master'] = {
                    'status': 'degraded',
                    'last_check': datetime.now().isoformat()
                }
        except Exception as e:
            master_status = {'error': str(e)}
            system_health['components']['master'] = {
                'status': 'unhealthy',
                'last_check': datetime.now().isoformat()
            }
        
        # Get crawler status
        crawler_healthy = False
        for node in CRAWLER_NODES:
            if node:
                try:
                    crawler_response = requests.get(f"{node}/health", timeout=3)
                    if crawler_response.status_code == 200:
                        crawler_status = crawler_response.json()
                        crawler_healthy = True
                        break
                except:
                    pass
                    
        system_health['components']['crawler'] = {
            'status': 'healthy' if crawler_healthy else 'unhealthy',
            'last_check': datetime.now().isoformat()
        }
        
        # Get indexer status
        indexer_healthy = False
        for node in INDEXER_NODES:
            if node:
                try:
                    indexer_response = requests.get(f"{node}/health", timeout=3)
                    if indexer_response.status_code == 200:
                        indexer_status = indexer_response.json()
                        indexer_healthy = True
                        break
                except:
                    pass
                    
        system_health['components']['indexer'] = {
            'status': 'healthy' if indexer_healthy else 'unhealthy',
            'last_check': datetime.now().isoformat()
        }
        
        # Check local indexer
        local_index_status = indexer.health_check()
        
        # Get queue statistics
        urls_queue = sqs.get_queue_attributes(
            QueueUrl=SQS_URLS_QUEUE,
            AttributeNames=['ApproximateNumberOfMessages']
        )
        content_queue = sqs.get_queue_attributes(
            QueueUrl=SQS_CONTENT_QUEUE,
            AttributeNames=['ApproximateNumberOfMessages']
        )
        
        # Update overall system health
        component_statuses = [c['status'] for c in system_health['components'].values()]
        if all(status == 'healthy' for status in component_statuses):
            system_health['status'] = 'healthy'
        elif any(status == 'unhealthy' for status in component_statuses):
            system_health['status'] = 'degraded'
        else:
            system_health['status'] = 'degraded'
            
        system_health['last_check'] = datetime.now().isoformat()
        
        return render_template('monitor.html',
            system_health=system_health,
            master_status=master_status,
            crawler_status=system_health['components']['crawler']['status'],
            indexer_status=system_health['components']['indexer']['status'],
            crawler_queue=urls_queue['Attributes']['ApproximateNumberOfMessages'],
            indexer_queue=content_queue['Attributes']['ApproximateNumberOfMessages'],
            local_index_status=local_index_status,
            last_update=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        )
    except Exception as e:
        system_health['status'] = 'degraded'
        system_health['last_check'] = datetime.now().isoformat()
        
        return render_template('monitor.html',
            error=f"Error fetching status: {str(e)}",
            system_health=system_health,
            last_update=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        )

@app.route('/health')
def health():
    """Health check endpoint for the webapp"""
    # Check the index health
    index_health = indexer.health_check()
    
    # Check AWS services
    aws_status = 'healthy'
    aws_errors = []
    
    try:
        # Check SQS
        sqs.get_queue_attributes(
            QueueUrl=SQS_URLS_QUEUE,
            AttributeNames=['ApproximateNumberOfMessages']
        )
        
        # Check S3
        s3.list_objects_v2(Bucket=S3_BUCKET, MaxKeys=1)
    except Exception as e:
        aws_status = 'degraded'
        aws_errors.append(str(e))
    
    health_status = {
        'status': 'healthy' if index_health['status'] == 'healthy' and aws_status == 'healthy' else 'degraded',
        'components': {
            'webapp': {
                'status': 'healthy',
                'uptime': str(datetime.now() - indexer.stats["start_time"]).split('.')[0]
            },
            'index': index_health,
            'aws': {
                'status': aws_status,
                'errors': aws_errors
            }
        },
        'timestamp': datetime.now().isoformat()
    }
    
    return jsonify(health_status)

@app.route('/status/crawler')
def crawler_status():
    """Get crawler component status"""
    # Try to get from crawler nodes first
    for node in CRAWLER_NODES:
        if node:
            try:
                response = requests.get(f"{node}/status", timeout=3)
                if response.status_code == 200:
                    return jsonify(response.json())
            except:
                pass
    
    # Fallback to queue metrics
    try:
        queue_attrs = sqs.get_queue_attributes(
            QueueUrl=SQS_URLS_QUEUE,
            AttributeNames=['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible']
        )
        
        return jsonify({
            'status': system_health['components']['crawler']['status'],
            'active_urls': queue_attrs['Attributes']['ApproximateNumberOfMessagesNotVisible'],
            'crawl_rate': 'N/A',
            'crawler_queue': queue_attrs['Attributes']['ApproximateNumberOfMessages'],
            'last_check': system_health['components']['crawler']['last_check']
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        })

@app.route('/status/indexer')
def indexer_status():
    """Get indexer component status"""
    # Try to get from indexer nodes first
    for node in INDEXER_NODES:
        if node:
            try:
                response = requests.get(f"{node}/status", timeout=3)
                if response.status_code == 200:
                    return jsonify(response.json())
            except:
                pass
    
    # Fallback to local index status
    try:
        index_stats = indexer.get_stats()
        queue_attrs = sqs.get_queue_attributes(
            QueueUrl=SQS_CONTENT_QUEUE,
            AttributeNames=['ApproximateNumberOfMessages']
        )
        
        return jsonify({
            'status': system_health['components']['indexer']['status'],
            'documents_indexed': index_stats['total_documents'],
            'indexing_rate': index_stats.get('documents_per_minute', 'N/A'),
            'indexer_queue': queue_attrs['Attributes']['ApproximateNumberOfMessages'],
            'last_check': system_health['components']['indexer']['last_check']
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        })

@app.route('/status/master')
def master_status():
    """Get master component status"""
    try:
        # Try to get from master node
        response = requests.get(f"{MASTER_NODE_URL}/status", timeout=5)
        if response.status_code == 200:
            return jsonify(response.json())
    except:
        pass
        
    # Fallback to basic status
    return jsonify({
        'status': system_health['components']['master']['status'],
        'total_urls': 'N/A',
        'system_health': system_health['status'],
        'last_update': system_health['last_check']
    })

if __name__ == '__main__':
    app.run(debug=True, port=5000)
