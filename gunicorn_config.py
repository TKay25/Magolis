import os

bind = f"0.0.0.0:{os.environ.get('PORT', 5000)}"
workers = 2
threads = 4
worker_class = "gthread"  # Use gthread instead of eventlet
timeout = 120
keepalive = 5
accesslog = "-"
errorlog = "-"
loglevel = "info"

preload_app = True
max_requests = 1000
max_requests_jitter = 50