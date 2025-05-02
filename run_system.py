import subprocess
import time
import os
import logging
import sys
import signal
from threading import Thread

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('SystemLauncher')

# Define components to run
components = [
    {"name": "Master Node", "command": ["python", "master_node.py"]},
    {"name": "Crawler Node", "command": ["python", "crawler_node.py"]},
    {"name": "Indexer Node", "command": ["python", "indexer_node.py"]},
    {"name": "Search Interface", "command": ["python", "search_interface.py"]}
]

processes = []

def run_component(component):
    """Run a component and monitor its output"""
    name = component["name"]
    cmd = component["command"]
    
    logger.info(f"Starting {name}: {' '.join(cmd)}")
    
    try:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True,
            bufsize=1
        )
        
        # Add process to global list
        processes.append(process)
        
        # Log process output
        for line in process.stdout:
            sys.stdout.write(f"[{name}] {line}")
            
        # If we get here, the process has terminated
        logger.warning(f"{name} terminated with exit code {process.returncode}")
        
    except Exception as e:
        logger.error(f"Error running {name}: {str(e)}")

def signal_handler(sig, frame):
    """Handle termination signals"""
    logger.info("Shutting down all components...")
    
    for process in processes:
        try:
            process.terminate()
        except:
            pass
    
    # Wait for processes to terminate
    time.sleep(2)
    
    # Force kill any remaining processes
    for process in processes:
        try:
            if process.poll() is None:
                process.kill()
        except:
            pass
    
    logger.info("All components shut down")
    sys.exit(0)

if __name__ == "__main__":
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    logger.info("Starting web crawler system...")
    
    # Create threads to run components
    threads = []
    for component in components:
        thread = Thread(target=run_component, args=(component,))
        thread.daemon = True
        threads.append(thread)
        thread.start()
        
        # Short delay between component starts
        time.sleep(1)
    
    # Print access instructions
    logger.info("\n" + "="*50)
    logger.info("Web Crawler System is running!")
    logger.info("Access the search interface at: http://localhost:5000")
    logger.info("Press Ctrl+C to stop all components")
    logger.info("="*50 + "\n")
    
    # Wait for threads
    try:
        while True:
            time.sleep(1)
            # Check if any threads have terminated
            alive_threads = sum(1 for t in threads if t.is_alive())
            if alive_threads < len(threads):
                logger.warning(f"Some components have terminated: {len(threads) - alive_threads} down")
    except KeyboardInterrupt:
        signal_handler(None, None)