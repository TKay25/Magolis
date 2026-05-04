"""
Unified Social Media Messaging System with Bulk Broadcast
Complete Flask Backend - Production Ready with PostgreSQL
"""
import eventlet
eventlet.monkey_patch()

import os
import json
import time
import threading
from datetime import datetime, timedelta
from functools import wraps
from flask import Flask, request, jsonify, render_template, session, g, send_from_directory, redirect, url_for
from flask_cors import CORS
from flask_socketio import SocketIO, emit
from werkzeug.security import generate_password_hash, check_password_hash
from dotenv import load_dotenv
import requests
import tweepy
import logging
from contextlib import contextmanager
import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Flask app
app = Flask(__name__, template_folder='templates', static_folder='static')
app.secret_key = os.getenv('SECRET_KEY', 'your-super-secret-key-change-this')
app.permanent_session_lifetime = timedelta(days=7)

# CORS
CORS(app, supports_credentials=True, origins=[
    "http://localhost:5000",
    "http://127.0.0.1:5000",
    "https://magolis.onrender.com"
])

socketio = SocketIO(app, cors_allowed_origins=[
    "http://localhost:5000",
    "http://127.0.0.1:5000",
    "https://magolis.onrender.com"
], async_mode='threading')

# ==================== DATABASE SETUP (POSTGRESQL) ====================

DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://lmsdatabase_8ag3_user:6WD9lOnHkiU7utlUUjT88m4XgEYQMTLb@dpg-ctp9h0aj1k6c739h9di0-a.oregon-postgres.render.com/lmsdatabase_8ag3')

def parse_db_url(url):
    """Parse PostgreSQL connection URL - handles missing port"""
    if url.startswith('postgresql://'):
        url = url[13:]
    
    # Split into credentials and rest
    credentials, rest = url.split('@')
    username, password = credentials.split(':')
    
    # Parse host_port and dbname - handle missing port
    host_port_db = rest.split('/')
    host_port = host_port_db[0]
    dbname = host_port_db[1] if len(host_port_db) > 1 else 'postgres'
    
    # Check if port is specified
    if ':' in host_port:
        host, port = host_port.split(':')
    else:
        host = host_port
        port = '5432'  # Default PostgreSQL port
    
    return {
        'dbname': dbname,
        'user': username,
        'password': password,
        'host': host,
        'port': port
    }

# Parse connection parameters
db_params = parse_db_url(DATABASE_URL)

# Create connection pool for background threads
db_pool = pool.SimpleConnectionPool(
    1, 10,
    dbname=db_params['dbname'],
    user=db_params['user'],
    password=db_params['password'],
    host=db_params['host'],
    port=db_params['port']
)

@contextmanager
def get_db_connection():
    """Get a database connection from the pool"""
    conn = db_pool.getconn()
    try:
        yield conn
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        db_pool.putconn(conn)

@contextmanager
def get_db_cursor(commit=True):
    """Get a database cursor"""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        try:
            yield cursor
            if commit:
                conn.commit()
        finally:
            cursor.close()

def init_db():
    """Initialize database tables"""
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            # Users table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY,
                    username TEXT UNIQUE NOT NULL,
                    password TEXT NOT NULL,
                    email TEXT,
                    full_name TEXT,
                    role TEXT DEFAULT 'user',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Contacts table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS contacts (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER,
                    platform TEXT NOT NULL,
                    platform_user_id TEXT NOT NULL,
                    display_name TEXT,
                    phone_number TEXT,
                    email TEXT,
                    opt_in BOOLEAN DEFAULT FALSE,
                    opt_in_date TIMESTAMP,
                    last_interaction TIMESTAMP,
                    tags TEXT,
                    notes TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(platform, platform_user_id)
                )
            ''')
            
            # Messages table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS messages (
                    id SERIAL PRIMARY KEY,
                    contact_id INTEGER REFERENCES contacts(id) ON DELETE CASCADE,
                    platform TEXT NOT NULL,
                    direction TEXT NOT NULL,
                    message TEXT NOT NULL,
                    status TEXT DEFAULT 'pending',
                    message_id TEXT,
                    sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    delivered_at TIMESTAMP,
                    read_at TIMESTAMP
                )
            ''')
            
            # Broadcasts table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS broadcasts (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER,
                    name TEXT NOT NULL,
                    platform TEXT NOT NULL,
                    message TEXT NOT NULL,
                    audience_filter TEXT,
                    total_recipients INTEGER DEFAULT 0,
                    sent_count INTEGER DEFAULT 0,
                    failed_count INTEGER DEFAULT 0,
                    status TEXT DEFAULT 'draft',
                    scheduled_for TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    completed_at TIMESTAMP
                )
            ''')
            
            # Broadcast recipients table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS broadcast_recipients (
                    id SERIAL PRIMARY KEY,
                    broadcast_id INTEGER REFERENCES broadcasts(id) ON DELETE CASCADE,
                    contact_id INTEGER REFERENCES contacts(id) ON DELETE CASCADE,
                    status TEXT DEFAULT 'pending',
                    error_message TEXT,
                    sent_at TIMESTAMP
                )
            ''')
            
            # Insert default admin user
            cursor.execute("SELECT id FROM users WHERE username = 'admin'")
            if not cursor.fetchone():
                cursor.execute('''
                    INSERT INTO users (username, password, email, full_name, role)
                    VALUES (%s, %s, %s, %s, %s)
                ''', ('admin', generate_password_hash('admin123'), 'admin@example.com', 'Administrator', 'admin'))
            
            conn.commit()
            logger.info("Database initialized successfully")

# Initialize database
init_db()

# ==================== HELPER FUNCTIONS FOR DATABASE OPERATIONS ====================

def save_contact(platform, platform_user_id, display_name=None, phone_number=None, opt_in=True):
    """Save or update contact - can be called from any thread"""
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO contacts (platform, platform_user_id, display_name, phone_number, opt_in, opt_in_date, last_interaction, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (platform, platform_user_id) 
                DO UPDATE SET 
                    display_name = COALESCE(EXCLUDED.display_name, contacts.display_name),
                    phone_number = COALESCE(EXCLUDED.phone_number, contacts.phone_number),
                    last_interaction = EXCLUDED.last_interaction,
                    updated_at = EXCLUDED.updated_at
                RETURNING id
            """, (
                platform, str(platform_user_id), display_name, phone_number, 
                opt_in, datetime.now() if opt_in else None, datetime.now(), 
                datetime.now(), datetime.now()
            ))
            result = cursor.fetchone()
            return result[0]

def save_message(contact_id, platform, direction, message, status='sent'):
    """Save a message - can be called from any thread"""
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO messages (contact_id, platform, direction, message, status, sent_at)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (contact_id, platform, direction, message, status, datetime.now()))

def get_recipients_for_broadcast(platform, audience_filter='all', tags=None):
    """Get recipients for broadcast - can be called from any thread"""
    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            query = "SELECT id, platform_user_id, display_name FROM contacts WHERE platform = %s AND opt_in = TRUE"
            params = [platform]
            
            if audience_filter == 'active':
                query += " AND last_interaction > NOW() - INTERVAL '30 days'"
            
            cursor.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]

def create_broadcast_record(user_id, name, platform, message, audience_filter, total_recipients):
    """Create a broadcast record - returns broadcast_id"""
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute('''
                INSERT INTO broadcasts (user_id, name, platform, message, audience_filter, total_recipients, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            ''', (user_id, name, platform, message, audience_filter, total_recipients, 'processing'))
            return cursor.fetchone()[0]

def update_broadcast_stats(broadcast_id, sent_count, failed_count):
    """Update broadcast stats - can be called from any thread"""
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute('''
                UPDATE broadcasts 
                SET sent_count = %s, failed_count = %s
                WHERE id = %s
            ''', (sent_count, failed_count, broadcast_id))

def complete_broadcast(broadcast_id):
    """Mark broadcast as completed - can be called from any thread"""
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute('''
                UPDATE broadcasts 
                SET status = 'completed', completed_at = %s
                WHERE id = %s
            ''', (datetime.now(), broadcast_id))

def add_broadcast_recipient(broadcast_id, contact_id, status, error_message=None):
    """Add broadcast recipient record - can be called from any thread"""
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute('''
                INSERT INTO broadcast_recipients (broadcast_id, contact_id, status, error_message, sent_at)
                VALUES (%s, %s, %s, %s, %s)
            ''', (broadcast_id, contact_id, status, error_message, datetime.now()))

def get_all_contacts(platform=None, opt_in_only=False, search=None):
    """Get contacts for display"""
    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            query = "SELECT * FROM contacts WHERE 1=1"
            params = []
            
            if platform:
                query += " AND platform = %s"
                params.append(platform)
            
            if opt_in_only:
                query += " AND opt_in = TRUE"
            
            if search:
                query += " AND (display_name ILIKE %s OR platform_user_id ILIKE %s OR phone_number ILIKE %s)"
                search_param = f"%{search}%"
                params.extend([search_param, search_param, search_param])
            
            query += " ORDER BY last_interaction DESC NULLS LAST LIMIT 100"
            
            cursor.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]

def get_messages(limit=50, platform=None):
    """Get message history"""
    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            query = '''
                SELECT m.*, c.display_name, c.platform_user_id
                FROM messages m
                JOIN contacts c ON m.contact_id = c.id
                WHERE 1=1
            '''
            params = []
            
            if platform:
                query += " AND m.platform = %s"
                params.append(platform)
            
            query += " ORDER BY m.sent_at DESC LIMIT %s"
            params.append(limit)
            
            cursor.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]

def get_broadcasts():
    """Get broadcast history"""
    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            cursor.execute('''
                SELECT * FROM broadcasts 
                ORDER BY created_at DESC 
                LIMIT 20
            ''')
            return [dict(row) for row in cursor.fetchall()]

def get_dashboard_stats():
    """Get dashboard statistics"""
    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            cursor.execute('SELECT COUNT(*) as total, SUM(CASE WHEN opt_in = TRUE THEN 1 ELSE 0 END) as opted_in FROM contacts')
            contact_stats = cursor.fetchone()
            
            cursor.execute('SELECT platform, COUNT(*) as count FROM contacts GROUP BY platform')
            contacts_by_platform = cursor.fetchall()
            
            cursor.execute('SELECT COUNT(*) as total, SUM(CASE WHEN direction = \'outgoing\' THEN 1 ELSE 0 END) as sent FROM messages')
            message_stats = cursor.fetchone()
            
            cursor.execute('SELECT COUNT(*) as total FROM broadcasts')
            broadcast_stats = cursor.fetchone()
            
            cursor.execute('''
                SELECT COUNT(*) as active FROM contacts 
                WHERE last_interaction > NOW() - INTERVAL '30 days'
            ''')
            active_stats = cursor.fetchone()
            
            return {
                'total_contacts': contact_stats['total'] if contact_stats else 0,
                'opted_in_contacts': contact_stats['opted_in'] if contact_stats else 0,
                'active_contacts_30d': active_stats['active'] if active_stats else 0,
                'sent_messages': message_stats['sent'] if message_stats else 0,
                'total_broadcasts': broadcast_stats['total'] if broadcast_stats else 0,
                'contacts_by_platform': [dict(row) for row in contacts_by_platform]
            }

# ==================== WEBHOOK ROUTE ====================

@app.route('/webhook/facebook', methods=['GET', 'POST'])
def facebook_webhook():
    """Facebook webhook endpoint - handles verification and incoming messages"""
    
    logger.info(f"📨 Webhook hit: Method={request.method}")
    
    if request.method == 'GET':
        mode = request.args.get('hub.mode')
        token = request.args.get('hub.verify_token')
        challenge = request.args.get('hub.challenge')
        
        logger.info(f"🔑 Verification request - Mode: {mode}, Token: {token}, Challenge: {challenge}")
        
        expected_token = os.getenv('FACEBOOK_VERIFY_TOKEN', 'fibonaccialucard123')
        
        if mode and token and mode == 'subscribe' and token == expected_token:
            logger.info("✅ Webhook verified successfully!")
            return challenge, 200
        
        logger.error(f"❌ Verification failed. Expected: {expected_token}, Got: {token}")
        return 'Verification failed', 403
    
    try:
        payload = request.json
        logger.info(f"📨 Facebook webhook POST received")
        
        if payload:
            entries = payload.get('entry', [])
            for entry in entries:
                messaging_events = entry.get('messaging', [])
                for event in messaging_events:
                    sender_id = event.get('sender', {}).get('id')
                    message = event.get('message', {})
                    
                    if message and sender_id:
                        content = message.get('text', '')
                        logger.info(f"💬 Message from {sender_id}: {content}")
                        
                        contact_id = save_contact(
                            platform='facebook',
                            platform_user_id=sender_id,
                            display_name=event.get('sender', {}).get('name', 'Facebook User'),
                            opt_in=True
                        )
                        
                        if content:
                            save_message(contact_id, 'facebook', 'incoming', content)
                        
                        socketio.emit('new_message', {
                            'platform': 'facebook',
                            'sender_id': sender_id,
                            'content': content,
                            'timestamp': datetime.now().isoformat()
                        })
                        
        return jsonify({'status': 'ok'}), 200
    except Exception as e:
        logger.error(f"Webhook error: {e}")
        return jsonify({'status': 'error'}), 500


# ==================== PLATFORM ADAPTERS ====================

class WhatsAppAdapter:
    def __init__(self):
        self.access_token = os.getenv('WHATSAPP_ACCESS_TOKEN')
        self.phone_number_id = os.getenv('WHATSAPP_PHONE_ID')
        self.is_configured = bool(self.access_token and self.phone_number_id)
    
    def send_message(self, recipient_id, content):
        if not self.is_configured:
            return {'success': False, 'error': 'WhatsApp not configured'}
        
        recipient = recipient_id if recipient_id.startswith('+') else f"+{recipient_id}"
        url = f"https://graph.facebook.com/v18.0/{self.phone_number_id}/messages"
        
        payload = {
            "messaging_product": "whatsapp",
            "to": recipient,
            "type": "text",
            "text": {"body": content}
        }
        
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }
        
        try:
            response = requests.post(url, json=payload, headers=headers, timeout=30)
            if response.status_code == 200:
                return {'success': True, 'platform': 'whatsapp'}
            return {'success': False, 'error': 'Failed to send'}
        except Exception as e:
            return {'success': False, 'error': str(e)}


class FacebookAdapter:
    def __init__(self):
        self.page_access_token = os.getenv('FACEBOOK_PAGE_TOKEN')
        self.page_id = os.getenv('FACEBOOK_PAGE_ID')
        self.is_configured = bool(self.page_access_token and self.page_id)
    
    def send_message(self, recipient_id, content):
        if not self.is_configured:
            return {'success': False, 'error': 'Facebook not configured'}
        
        url = "https://graph.facebook.com/v18.0/me/messages"
        payload = {
            "recipient": {"id": recipient_id},
            "message": {"text": content},
            "messaging_type": "RESPONSE"
        }
        
        headers = {
            "Authorization": f"Bearer {self.page_access_token}",
            "Content-Type": "application/json"
        }
        
        try:
            response = requests.post(url, json=payload, headers=headers, timeout=30)
            if response.status_code == 200:
                return {'success': True, 'platform': 'facebook'}
            return {'success': False, 'error': 'Failed to send'}
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def get_page_id(self):
        return self.page_id
    
    def get_conversations(self, limit=50):
        page_id = self.get_page_id()
        if not page_id:
            return {'success': False, 'error': 'Could not get Page ID'}
        
        url = f"https://graph.facebook.com/v18.0/{page_id}/conversations"
        params = {
            'access_token': self.page_access_token,
            'fields': 'participants,updated_time,message_count,messages.limit(1){message,created_time}',
            'limit': limit
        }
        
        try:
            response = requests.get(url, params=params)
            data = response.json()
            
            if 'error' in data:
                return {'success': False, 'error': data['error']['message']}
            
            conversations = []
            for conv in data.get('data', []):
                participants = conv.get('participants', {}).get('data', [])
                user_participant = None
                for p in participants:
                    if p.get('id') != page_id:
                        user_participant = p
                        break
                
                if user_participant:
                    messages_data = conv.get('messages', {}).get('data', [])
                    last_message = messages_data[0] if messages_data else None
                    
                    conversations.append({
                        'psid': user_participant['id'],
                        'name': user_participant.get('name', 'Facebook User'),
                        'last_message': last_message.get('message', '') if last_message else None,
                        'last_interaction': conv.get('updated_time')
                    })
            
            return {'success': True, 'conversations': conversations}
        except Exception as e:
            return {'success': False, 'error': str(e)}


class TwitterAdapter:
    def __init__(self):
        self.bearer_token = os.getenv('TWITTER_BEARER_TOKEN')
        self.api_key = os.getenv('TWITTER_API_KEY')
        self.api_secret = os.getenv('TWITTER_API_SECRET')
        self.access_token = os.getenv('TWITTER_ACCESS_TOKEN')
        self.access_secret = os.getenv('TWITTER_ACCESS_SECRET')
        self.is_configured = bool(self.bearer_token)
        self.client = None
        
        if self.is_configured:
            try:
                self.client = tweepy.Client(
                    bearer_token=self.bearer_token,
                    consumer_key=self.api_key,
                    consumer_secret=self.api_secret,
                    access_token=self.access_token,
                    access_token_secret=self.access_secret
                )
            except:
                self.is_configured = False
    
    def send_message(self, recipient_id, content):
        if not self.is_configured or not self.client:
            return {'success': False, 'error': 'Twitter not configured'}
        
        try:
            response = self.client.send_direct_message(participant_id=recipient_id, text=content)
            if response and response.data:
                return {'success': True, 'platform': 'twitter'}
            return {'success': False, 'error': 'Failed to send DM'}
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def get_user_id(self, username):
        if not self.client:
            return None
        try:
            user = self.client.get_user(username=username.lstrip('@'))
            return user.data.id if user and user.data else None
        except:
            return None

# Instagram sync endpoint
@app.route('/api/instagram/sync-contacts', methods=['POST'])
def sync_instagram_contacts():
    """Fetch all Instagram DM conversations and sync to contacts database"""
    try:
        instagram_adapter = adapters['instagram']
        
        # Detailed configuration check
        if not instagram_adapter.is_configured:
            missing = []
            if not os.getenv('INSTAGRAM_BUSINESS_ID'):
                missing.append("INSTAGRAM_BUSINESS_ID")
            if not os.getenv('FACEBOOK_PAGE_TOKEN'):
                missing.append("FACEBOOK_PAGE_TOKEN")
            
            error_msg = f"Instagram not configured. Missing: {', '.join(missing)}"
            logger.error(error_msg)
            return jsonify({
                'success': False, 
                'error': error_msg,
                'missing': missing
            }), 400
        
        # Log what we have (safely)
        logger.info(f"Instagram Business ID: {instagram_adapter.business_id}")
        logger.info(f"Page Token present: {bool(instagram_adapter.access_token)}")
        
        result = instagram_adapter.get_conversations(limit=100)
        
        # Return the actual error from Instagram API
        if not result['success']:
            logger.error(f"Instagram API error: {result['error']}")
            return jsonify({
                'success': False, 
                'error': result['error'],
                'details': result.get('details', {})
            }), 400
        
        synced_count = 0
        new_contacts = []
        
        for conv in result['conversations']:
            psid = conv['psid']
            user_name = conv['name']
            
            contact_id = save_contact(
                platform='instagram',
                platform_user_id=psid,
                display_name=user_name,
                opt_in=True
            )
            
            synced_count += 1
            new_contacts.append({
                'id': contact_id,
                'psid': psid,
                'name': user_name,
                'last_message': conv.get('last_message')
            })
        
        return jsonify({
            'success': True,
            'synced': synced_count,
            'contacts': new_contacts,
            'message': f'Successfully synced {synced_count} Instagram contacts'
        })
        
    except Exception as e:
        logger.error(f"Instagram sync error: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False, 
            'error': str(e),
            'traceback': traceback.format_exc()
        }), 500
    
# Instagram conversation endpoint
@app.route('/api/instagram/conversations/<psid>', methods=['GET'])
def get_instagram_conversation(psid):
    instagram_adapter = adapters['instagram']
    if not instagram_adapter.is_configured:
        return jsonify({'success': False, 'error': 'Instagram not configured'}), 400
    
    messages = instagram_adapter.get_conversation_history(psid, limit=100)
    
    return jsonify({
        'success': True,
        'messages': messages,
        'count': len(messages)
    })

@app.route('/webhook/instagram', methods=['GET', 'POST'])
def instagram_webhook():
    if request.method == 'GET':
        # Verification
        mode = request.args.get('hub.mode')
        token = request.args.get('hub.verify_token')
        challenge = request.args.get('hub.challenge')
        
        expected_token = os.getenv('FACEBOOK_VERIFY_TOKEN', 'fibonaccialucard123')
        
        if mode and token and mode == 'subscribe' and token == expected_token:
            return challenge, 200
        return 'Verification failed', 403
    
    try:
        payload = request.json
        logger.info(f"📨 Instagram webhook received")
        
        # Process comments and messages
        entries = payload.get('entry', [])
        for entry in entries:
            # Handle message events
            messaging = entry.get('messaging', [])
            for event in messaging:
                sender_id = event.get('sender', {}).get('id')
                message = event.get('message', {})
                
                if message and sender_id:
                    content = message.get('text', '')
                    contact_id = save_contact(
                        platform='instagram',
                        platform_user_id=sender_id,
                        display_name='Instagram User',
                        opt_in=True
                    )
                    if content:
                        save_message(contact_id, 'instagram', 'incoming', content)
                    
                    socketio.emit('new_message', {
                        'platform': 'instagram',
                        'sender_id': sender_id,
                        'content': content,
                        'timestamp': datetime.now().isoformat()
                    })
            
            # Handle comment events (for comment-to-DM automation)
            changes = entry.get('changes', [])
            for change in changes:
                if change.get('field') == 'comments':
                    comment_data = change.get('value', {})
                    # Save comment and optionally auto-reply
                    # This is where you'd implement ManyChat-style automation[citation:2][citation:7]
        
        return jsonify({'status': 'ok'}), 200
    except Exception as e:
        logger.error(f"Webhook error: {e}")
        return jsonify({'status': 'error'}), 500

class InstagramAdapter:
    def __init__(self):
        self.access_token = os.getenv('FACEBOOK_PAGE_TOKEN')
        self.business_id = os.getenv('INSTAGRAM_BUSINESS_ID')
        self.is_configured = bool(self.access_token and self.business_id)
        
        # Log configuration status
        if self.is_configured:
            logger.info(f"Instagram Adapter: Configured with Business ID: {self.business_id}")
        else:
            logger.warning(f"Instagram Adapter: Not configured. Token: {bool(self.access_token)}, Business ID: {bool(self.business_id)}")
    
    def get_conversations(self, limit=50):
        """Fetch Instagram DM conversations"""
        if not self.is_configured:
            missing = []
            if not self.access_token:
                missing.append("FACEBOOK_PAGE_TOKEN")
            if not self.business_id:
                missing.append("INSTAGRAM_BUSINESS_ID")
            return {'success': False, 'error': f'Missing: {", ".join(missing)}'}
        
        url = f"https://graph.facebook.com/v18.0/{self.business_id}/conversations"
        params = {
            'access_token': self.access_token,
            'fields': 'participants,updated_time,messages.limit(1){message,created_time}',
            'limit': limit
        }
        
        logger.info(f"Instagram API Request URL: {url}")
        
        try:
            response = requests.get(url, params=params)
            data = response.json()
            
            # Log the full response for debugging
            logger.info(f"Instagram API Response Status: {response.status_code}")
            logger.info(f"Instagram API Response: {json.dumps(data, indent=2)}")
            
            if response.status_code != 200:
                error_msg = data.get('error', {}).get('message', 'Unknown error')
                error_code = data.get('error', {}).get('code', 0)
                error_subcode = data.get('error', {}).get('error_subcode', 0)
                return {
                    'success': False, 
                    'error': f"[{error_code}:{error_subcode}] {error_msg}",
                    'details': data.get('error', {})
                }
            
            if 'error' in data:
                return {'success': False, 'error': data['error']['message']}
            
            conversations = []
            for conv in data.get('data', []):
                participants = conv.get('participants', {}).get('data', [])
                user_participant = None
                for p in participants:
                    if p.get('id') != self.business_id:
                        user_participant = p
                        break
                
                if user_participant:
                    messages_data = conv.get('messages', {}).get('data', [])
                    last_message = messages_data[0] if messages_data else None
                    
                    conversations.append({
                        'psid': user_participant['id'],
                        'name': user_participant.get('name', 'Instagram User'),
                        'last_message': last_message.get('message', '') if last_message else None,
                        'last_interaction': conv.get('updated_time')
                    })
            
            return {'success': True, 'conversations': conversations}
        except Exception as e:
            logger.error(f"Instagram API exception: {str(e)}")
            return {'success': False, 'error': str(e)}

class LinkedInAdapter:
    def __init__(self):
        self.access_token = os.getenv('LINKEDIN_ACCESS_TOKEN')
        self.is_configured = bool(self.access_token)
    
    def send_message(self, recipient_id, content):
        if not self.is_configured:
            return {'success': False, 'error': 'LinkedIn not configured'}
        return {'success': False, 'error': 'LinkedIn API coming soon'}


# Initialize adapters
adapters = {
    'whatsapp': WhatsAppAdapter(),
    'facebook': FacebookAdapter(),
    'twitter': TwitterAdapter(),
    'instagram': InstagramAdapter(),
    'linkedin': LinkedInAdapter()
}

# ==================== AUTH ROUTES ====================

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            return jsonify({'success': False, 'error': 'Authentication required'}), 401
        return f(*args, **kwargs)
    return decorated_function

@app.route('/login')
def login_page():
    return render_template('login.html')

@app.route('/')
def index():
    if 'user_id' not in session:
        return redirect('/login')
    return render_template('index.html')

@app.route('/api/login', methods=['POST'])
def api_login():
    data = request.json
    username = data.get('username')
    password = data.get('password')
    
    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            cursor.execute("SELECT id, username, password, full_name, role FROM users WHERE username = %s", (username,))
            user = cursor.fetchone()
    
    if user and check_password_hash(user['password'], password):
        session.permanent = True
        session['user_id'] = user['id']
        session['username'] = user['username']
        session['full_name'] = user['full_name']
        session['role'] = user['role']
        
        return jsonify({
            'success': True,
            'user': {
                'id': user['id'],
                'username': user['username'],
                'full_name': user['full_name'],
                'role': user['role']
            }
        })
    
    return jsonify({'success': False, 'error': 'Invalid credentials'}), 401

@app.route('/api/logout', methods=['POST'])
def api_logout():
    session.clear()
    return jsonify({'success': True})

@app.route('/api/check-auth', methods=['GET'])
def check_auth():
    if 'user_id' in session:
        return jsonify({
            'authenticated': True,
            'user': {
                'id': session['user_id'],
                'username': session.get('username'),
                'full_name': session.get('full_name'),
                'role': session.get('role', 'user')
            }
        })
    return jsonify({'authenticated': False}), 401

# ==================== FACEBOOK CONTACT SYNC ROUTES ====================

@app.route('/api/facebook/sync-contacts', methods=['POST'])
@login_required
def sync_facebook_contacts():
    try:
        facebook_adapter = adapters['facebook']
        
        if not facebook_adapter.is_configured:
            logger.error("Facebook not configured - missing PAGE_TOKEN or PAGE_ID")
            return jsonify({
                'success': False, 
                'error': 'Facebook not configured. Please add FACEBOOK_PAGE_TOKEN and FACEBOOK_PAGE_ID to environment variables.'
            }), 400
        
        result = facebook_adapter.get_conversations(limit=100)
        
        if not result['success']:
            logger.error(f"Facebook API error: {result['error']}")
            return jsonify({'success': False, 'error': result['error']}), 400
        
        synced_count = 0
        new_contacts = []
        
        for conv in result['conversations']:
            psid = conv['psid']
            user_name = conv['name']
            
            contact_id = save_contact(
                platform='facebook',
                platform_user_id=psid,
                display_name=user_name,
                opt_in=True
            )
            
            synced_count += 1
            new_contacts.append({
                'id': contact_id,
                'psid': psid,
                'name': user_name,
                'last_message': conv.get('last_message')
            })
        
        return jsonify({
            'success': True,
            'synced': synced_count,
            'contacts': new_contacts,
            'message': f'Successfully synced {synced_count} Facebook contacts'
        })
        
    except Exception as e:
        logger.error(f"Sync error: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/facebook/conversations/<psid>', methods=['GET'])
@login_required
def get_facebook_conversation(psid):
    facebook_adapter = adapters['facebook']
    if not facebook_adapter.is_configured:
        return jsonify({'success': False, 'error': 'Facebook not configured'}), 400
    
    page_id = facebook_adapter.get_page_id()
    if not page_id:
        return jsonify({'success': False, 'error': 'Could not get Page ID'}), 400
    
    try:
        conv_url = f"https://graph.facebook.com/v18.0/{page_id}/conversations"
        params = {
            'access_token': facebook_adapter.page_access_token,
            'filter': 'participants',
            'user_id': psid,
            'fields': 'id'
        }
        
        response = requests.get(conv_url, params=params)
        data = response.json()
        
        if not data.get('data'):
            return jsonify({'success': True, 'messages': [], 'message': 'No conversation found'})
        
        conversation_id = data['data'][0]['id']
        
        messages_url = f"https://graph.facebook.com/v18.0/{conversation_id}/messages"
        msg_params = {
            'access_token': facebook_adapter.page_access_token,
            'fields': 'message,created_time,from,id',
            'limit': 100
        }
        
        msg_response = requests.get(messages_url, params=msg_params)
        messages_data = msg_response.json()
        
        messages = []
        for msg in messages_data.get('data', []):
            messages.append({
                'id': msg.get('id'),
                'content': msg.get('message', ''),
                'timestamp': msg.get('created_time'),
                'direction': 'incoming' if msg.get('from', {}).get('id') != page_id else 'outgoing',
                'sender_name': msg.get('from', {}).get('name', 'Unknown')
            })
        
        return jsonify({
            'success': True,
            'messages': messages,
            'count': len(messages)
        })
        
    except Exception as e:
        logger.error(f"Error fetching conversation: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500

# ==================== MESSAGING ROUTES ====================

@app.route('/api/send', methods=['POST'])
@login_required
def send_message():
    data = request.json
    platform = data.get('platform')
    recipient = data.get('recipient_id')
    content = data.get('message')
    display_name = data.get('display_name')
    
    if not platform or not recipient or not content:
        return jsonify({'success': False, 'error': 'Missing required fields'}), 400
    
    if platform not in adapters:
        return jsonify({'success': False, 'error': f'Invalid platform: {platform}'}), 400
    
    if platform == 'twitter' and not recipient.isdigit():
        user_id = adapters['twitter'].get_user_id(recipient)
        if not user_id:
            return jsonify({'success': False, 'error': 'Could not find Twitter user'}), 400
        recipient = user_id
    
    result = adapters[platform].send_message(recipient, content)
    
    if result.get('success'):
        contact_id = save_contact(platform, recipient, display_name, recipient if platform == 'whatsapp' else None, True)
        save_message(contact_id, platform, 'outgoing', content)
        
        socketio.emit('message_sent', {
            'platform': platform,
            'recipient': recipient,
            'content': content[:100],
            'timestamp': datetime.now().isoformat()
        })
    
    return jsonify(result)

@app.route('/api/broadcast', methods=['POST'])
@login_required
def broadcast_message():
    data = request.json
    platform = data.get('platform')
    message = data.get('message')
    audience_filter = data.get('audience_filter', 'all')
    tags = data.get('tags')
    campaign_name = data.get('campaign_name', f"Broadcast {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    rate_limit = data.get('rate_limit', 1)
    
    if not platform or not message:
        return jsonify({'success': False, 'error': 'Missing required fields'}), 400
    
    if platform not in adapters:
        return jsonify({'success': False, 'error': f'Invalid platform: {platform}'}), 400
    
    adapter = adapters[platform]
    if not adapter.is_configured:
        return jsonify({'success': False, 'error': f'{platform} is not configured'}), 400
    
    recipients = get_recipients_for_broadcast(platform, audience_filter, tags)
    
    if not recipients:
        return jsonify({'success': False, 'error': 'No recipients found matching criteria'}), 404
    
    broadcast_id = create_broadcast_record(
        session['user_id'], campaign_name, platform, message, 
        audience_filter, len(recipients)
    )
    
    def process_broadcast():
        sent_count = 0
        failed_count = 0
        
        for i, recipient in enumerate(recipients):
            result = adapter.send_message(recipient['platform_user_id'], message)
            
            add_broadcast_recipient(
                broadcast_id, 
                recipient['id'], 
                'sent' if result.get('success') else 'failed', 
                result.get('error')
            )
            
            if result.get('success'):
                sent_count += 1
                save_message(recipient['id'], platform, 'outgoing', message)
            else:
                failed_count += 1
            
            update_broadcast_stats(broadcast_id, sent_count, failed_count)
            
            if i < len(recipients) - 1:
                time.sleep(rate_limit)
        
        complete_broadcast(broadcast_id)
        
        socketio.emit('broadcast_completed', {
            'broadcast_id': broadcast_id,
            'sent': sent_count,
            'failed': failed_count,
            'total': len(recipients)
        })
    
    thread = threading.Thread(target=process_broadcast)
    thread.daemon = True
    thread.start()
    
    return jsonify({
        'success': True,
        'broadcast_id': broadcast_id,
        'total_recipients': len(recipients),
        'message': f'Broadcast started. Sending to {len(recipients)} recipients.'
    })

# ==================== CONTACT ROUTES ====================

@app.route('/api/contacts', methods=['GET'])
@login_required
def get_contacts():
    platform = request.args.get('platform')
    opt_in_only = request.args.get('opt_in_only', 'false').lower() == 'true'
    search = request.args.get('search', '')
    
    contacts = get_all_contacts(platform, opt_in_only, search)
    
    return jsonify({
        'success': True,
        'contacts': contacts,
        'count': len(contacts)
    })

@app.route('/api/contacts/<int:contact_id>', methods=['PUT'])
@login_required
def update_contact(contact_id):
    data = request.json
    
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute('''
                UPDATE contacts 
                SET display_name = COALESCE(%s, display_name),
                    phone_number = COALESCE(%s, phone_number),
                    email = COALESCE(%s, email),
                    tags = COALESCE(%s, tags),
                    notes = COALESCE(%s, notes),
                    opt_in = COALESCE(%s, opt_in),
                    updated_at = %s
                WHERE id = %s
            ''', (
                data.get('display_name'),
                data.get('phone_number'),
                data.get('email'),
                data.get('tags'),
                data.get('notes'),
                data.get('opt_in'),
                datetime.now(),
                contact_id
            ))
    
    return jsonify({'success': True, 'message': 'Contact updated'})

@app.route('/api/contacts/<int:contact_id>', methods=['DELETE'])
@login_required
def delete_contact(contact_id):
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("DELETE FROM contacts WHERE id = %s", (contact_id,))
    
    return jsonify({'success': True, 'message': 'Contact deleted'})

@app.route('/api/contacts/bulk-opt-in', methods=['POST'])
@login_required
def bulk_opt_in():
    data = request.json
    contact_ids = data.get('contact_ids', [])
    
    if not contact_ids:
        return jsonify({'success': False, 'error': 'No contacts selected'}), 400
    
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            placeholders = ','.join(['%s'] * len(contact_ids))
            cursor.execute(f'''
                UPDATE contacts 
                SET opt_in = TRUE, opt_in_date = %s, updated_at = %s
                WHERE id IN ({placeholders})
            ''', [datetime.now(), datetime.now()] + contact_ids)
    
    return jsonify({'success': True, 'updated': len(contact_ids)})

# ==================== MESSAGE HISTORY ====================

"""def get_long_lived_user_token(short_lived_token):
    # IMPORTANT: Never hardcode secrets in production code. Use environment variables.
    app_id = "122310148634227900"
    app_secret = "aea161e21e6008e9175f26e8f20cd732"

    url = (f"https://graph.facebook.com/v20.0/oauth/access_token"
           f"?grant_type=fb_exchange_token"
           f"&client_id={app_id}"
           f"&client_secret={app_secret}"
           f"&fb_exchange_token={short_lived_token}")

    response = requests.get(url)
    data = response.json()

    if 'access_token' in data:
        print(f"Long-lived token: {data['access_token']}")
        print(f"Expires in: {data['expires_in']} seconds (~{int(data['expires_in']/86400)} days)")
        return data['access_token']
    else:
        print(f"Error: {data}")
        return None

# Example usage with your short-lived token
short_token = "EAAVWG0GVTr4BReJwcZA06R5DYDZCUZCVegr6yC5fPQZCzHIjUBEWHYm4OFlX3FLUi4tH7CroBEWw3ql08HOfO1CBHFa4heP00iZA9outvG0AhIoJifjETtWicG7Sr7XAynhlnLHvvjce7RVuqdfU0ZCVimRoFJOn84unrqAUonS16M6EeuYKBOHZAjIglRjWBtvvp37eEzNj8cYi5LjY5fGBSas9ksTcwEu7beXRcp3U1csIQMNu1VCZAB9uaZAsd0jNMe8Xb0Mn3r1WZBC4QZD"
long_lived_token = get_long_lived_user_token(short_token)"""

@app.route('/api/messages', methods=['GET'])
@login_required
def get_messages_api():
    limit = request.args.get('limit', 50, type=int)
    platform = request.args.get('platform')
    
    messages = get_messages(limit, platform)
    
    return jsonify({
        'success': True,
        'messages': messages,
        'count': len(messages)
    })

# ==================== BROADCAST ROUTES ====================

@app.route('/api/broadcasts', methods=['GET'])
@login_required
def get_broadcasts_api():
    broadcasts = get_broadcasts()
    
    return jsonify({
        'success': True,
        'broadcasts': broadcasts
    })

@app.route('/api/broadcasts/<int:broadcast_id>', methods=['GET'])
@login_required
def get_broadcast_details(broadcast_id):
    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            cursor.execute("SELECT * FROM broadcasts WHERE id = %s", (broadcast_id,))
            broadcast = cursor.fetchone()
            
            if not broadcast:
                return jsonify({'success': False, 'error': 'Broadcast not found'}), 404
            
            cursor.execute('''
                SELECT br.*, c.display_name, c.platform_user_id
                FROM broadcast_recipients br
                JOIN contacts c ON br.contact_id = c.id
                WHERE br.broadcast_id = %s
            ''', (broadcast_id,))
            recipients = cursor.fetchall()
    
    return jsonify({
        'success': True,
        'broadcast': dict(broadcast),
        'recipients': [dict(r) for r in recipients]
    })

# ==================== STATUS ROUTES ====================

@app.route('/api/status', methods=['GET'])
def get_platform_status():
    status = {}
    for name, adapter in adapters.items():
        status[name] = {
            'configured': adapter.is_configured,
            'platform': name,
            'name': name.capitalize()
        }
    return jsonify(status)

@app.route('/api/dashboard/stats', methods=['GET'])
@login_required
def get_dashboard_stats_api():
    stats = get_dashboard_stats()
    return jsonify({'success': True, 'stats': stats})

@app.route('/api/recipient-count', methods=['POST'])
@login_required
def get_recipient_count():
    data = request.json
    platform = data.get('platform')
    audience_filter = data.get('audience_filter', 'all')
    tags = data.get('tags')
    
    recipients = get_recipients_for_broadcast(platform, audience_filter, tags)
    return jsonify({'success': True, 'count': len(recipients)})

# ==================== HEALTH CHECK ====================

@app.route('/health', methods=['GET'])
def health_check():
    configured_count = sum(1 for a in adapters.values() if a.is_configured)
    return jsonify({
        'status': 'healthy',
        'configured_platforms': configured_count,
        'total_platforms': len(adapters),
        'timestamp': datetime.now().isoformat()
    })

# ==================== SOCKET.IO EVENTS ====================

@socketio.on('connect')
def handle_connect():
    emit('connected', {'message': 'Connected to server'})

# ==================== RUN THE APP ====================

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    debug = os.environ.get('FLASK_DEBUG', 'False').lower() == 'true'
    
    print("=" * 60)
    print("UNIFIED SOCIAL MEDIA MESSAGING SYSTEM")
    print("=" * 60)
    print(f"\nServer running on port: {port}")
    print(f"Debug mode: {debug}")
    print("\nConfigured Platforms:")
    for name, adapter in adapters.items():
        status = "✓ CONFIGURED" if adapter.is_configured else "✗ NOT CONFIGURED"
        print(f"  • {name.upper()}: {status}")
    
    print("\nDefault Admin Login:")
    print("  Username: admin")
    print("  Password: admin123")
    print("\n" + "=" * 60)
    
    socketio.run(app, host='0.0.0.0', port=port, debug=debug)