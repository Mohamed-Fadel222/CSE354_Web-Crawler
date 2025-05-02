from flask import Flask, render_template, request, redirect, url_for
import boto3, json, os
from dotenv import load_dotenv
from whoosh.index import open_dir
from whoosh.qparser import QueryParser

# ─── Config ───────────────────────────────────────────────────────────
load_dotenv()
AWS_REGION    = os.getenv('AWS_REGION')
SQS_URLS_QUEUE = os.getenv('SQS_URLS_QUEUE')
S3_BUCKET     = os.getenv('S3_BUCKET')

# ─── AWS Clients ──────────────────────────────────────────────────────
sqs = boto3.client('sqs', region_name=AWS_REGION)
# (we’ll use S3 below in the crawler)

# ─── Simple Whoosh wrapper for search ─────────────────────────────────
class SimpleIndexer:
    def __init__(self, index_dir='index'):
        self.index = open_dir(index_dir)
    def search(self, text):
        with self.index.searcher() as searcher:
            query   = QueryParser('content', self.index.schema).parse(text)
            results = searcher.search(query, limit=20)
            return [hit['url'] for hit in results]

indexer = SimpleIndexer()

# ─── Flask app ────────────────────────────────────────────────────────
app = Flask(__name__)

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
    return render_template('index.html', sent=sent)

@app.route('/search', methods=['GET','POST'])
def search():
    results = []
    query   = ''
    if request.method == 'POST':
        query = request.form.get('query','')
        results = indexer.search(query)
    return render_template('search.html', query=query, results=results)

if __name__ == '__main__':
    # Runs on http://localhost:5000
    app.run(debug=True)
