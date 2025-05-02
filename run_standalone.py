#!/usr/bin/env python
import os
import logging
import urllib3
import warnings
import sys

# Set environment variables before importing the app
os.environ['USE_LOCAL_STORAGE'] = 'true'

# Import the app
from standalone_web_app import app, ensure_index_exists

# Suppress warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings('ignore')

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('StandaloneWebAppRunner')

if __name__ == '__main__':
    # Get command line arguments
    use_local = True  # Default to true
    port = 5000  # Default port
    
    # Parse command line arguments
    for arg in sys.argv[1:]:
        if arg == '--use-s3':
            use_local = False
        elif arg.startswith('--port='):
            try:
                port = int(arg.split('=')[1])
            except (ValueError, IndexError):
                logger.error(f"Invalid port number in argument: {arg}")
    
    # Set environment variable
    os.environ['USE_LOCAL_STORAGE'] = str(use_local).lower()
    
    # Print startup message
    print("\n" + "="*60)
    print(" WEB CRAWLER & SEARCH ENGINE - STANDALONE MODE")
    print("="*60)
    print(" Features:")
    print(" - Search indexed content")
    print(" - Add new URLs to crawl directly from the interface")
    print(" - Fix content retrieval errors with one click")
    print(f"\n Storage mode: {'LOCAL' if use_local else 'AWS S3'}")
    print(f" Access the web interface at: http://localhost:{port}")
    print(" Press Ctrl+C to stop the server")
    print(" Command line options:")
    print("   --use-s3        Use AWS S3 for storage (default is local storage)")
    print("   --port=XXXX     Use specified port (default is 5000)")
    print("="*60 + "\n")
    
    # Ensure the index exists
    ensure_index_exists()
    
    # Start the Flask app
    app.run(debug=True, host='0.0.0.0', port=port) 