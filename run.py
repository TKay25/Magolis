#!/usr/bin/env python3
"""
Unified Social Media Messaging System
Production entry point with proper eventlet support
"""

import os
import sys

# Ensure eventlet is monkey patched FIRST before anything else
try:
    import eventlet
    eventlet.monkey_patch()
    print("✅ Eventlet monkey patching applied")
except ImportError:
    print("⚠️ Eventlet not available - WebSocket support will be limited")
    pass

# Then import the app
from app import app, socketio

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    debug = os.environ.get("FLASK_DEBUG", "False").lower() == "true"
    
    socketio.run(
        app,
        host="0.0.0.0",
        port=port,
        debug=debug,
        allow_unsafe_werkzeug=True  # For development only
    )