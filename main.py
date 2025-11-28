import os
import subprocess
import signal
import sys
import time
import logging
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global list to track subprocesses
processes = []

def signal_handler(sig, frame):
    """Handle Ctrl+C gracefully by terminating all subprocesses."""
    logger.info("\n⏹️  Shutting down all CDC pipelines...")
    for proc in processes:
        proc.terminate()
    
    # Wait for all processes to terminate
    for proc in processes:
        proc.wait()
    
    logger.info("👋 All pipelines stopped. Goodbye!")
    sys.exit(0)

if __name__ == "__main__":
    print("\n✅ Starting Multi-Table CDC Pipeline...")
    print("📊 Monitoring MySQL changes across 4 tables...\n")
    
    # Register signal handler for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        # Start all subprocesses
        scripts = [
            "transaction.py",
            "api_logs.py",
            "chargeback.py",
            "refund_requests.py"
        ]
        
        for script in scripts:
            logger.info(f"🚀 Starting {script}...")
            proc = subprocess.Popen(
                ["python3", "-u", script],
                # stdout=subprocess.PIPE,
                # stderr=subprocess.PIPE
                stdout=sys.stdout,     # <- send to container stdout
                stderr=sys.stderr      # <- send to container stderr
            )
            processes.append(proc)
            time.sleep(2)  # Stagger startup to avoid resource conflicts
        
        logger.info(f"✅ All {len(processes)} CDC pipelines started successfully!\n")
        
        # Keep main process alive and monitor subprocesses
        while True:
            # Check if any subprocess has died
            for i, proc in enumerate(processes):
                retcode = proc.poll()
                if retcode is not None:
                    logger.error(f"❌ Process {scripts[i]} exited with code {retcode}")
                    # Optionally restart the failed process
                    logger.info(f"🔄 Restarting {scripts[i]}...")
                    new_proc = subprocess.Popen(
                        ["python3", scripts[i]],
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE
                    )
                    processes[i] = new_proc
            
            time.sleep(5)  # Check every 5 seconds
            
    except KeyboardInterrupt:
        signal_handler(None, None)
    except Exception as e:
        logger.error(f"❌ Main process error: {e}")
        signal_handler(None, None)