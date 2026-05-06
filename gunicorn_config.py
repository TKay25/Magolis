import os
import multiprocessing

bind = f"0.0.0.0:{os.environ.get('PORT', 5000)}"
workers = 1  # eventlet requires single worker
worker_class = "eventlet"  # Use eventlet worker for WebSocket support
timeout = 120
keepalive = 5
accesslog = "-"
errorlog = "-"
loglevel = "info"

# For better WebSocket performance
worker_connections = 1000
max_requests = 1000
max_requests_jitter = 50