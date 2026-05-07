#!/usr/bin/env python3
"""
Unified Social Media Messaging System - FULLY CORRECTED
Fixed database connection issues (no connection pool)
All original features preserved: webhooks, Instagram API, Twitter, WhatsApp, etc.
"""

import os
import sys
import json
import time
import threading
import warnings
import csv
import io

# Suppress deprecation warnings
warnings.filterwarnings("ignore", category=SyntaxWarning)

from datetime import datetime, timedelta
from functools import wraps
from flask import Flask, request, jsonify, session, redirect, url_for, render_template
from flask_cors import CORS
from flask_socketio import SocketIO, emit
from werkzeug.security import generate_password_hash, check_password_hash
from dotenv import load_dotenv
import requests

# Fix tweepy import for Python 3.11+
try:
    import tweepy
except ImportError:
    tweepy = None
    print("Warning: tweepy not installed")

import logging
from contextlib import contextmanager
import psycopg2
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

# SocketIO with threading mode
socketio = SocketIO(
    app,
    cors_allowed_origins=[
        "http://localhost:5000",
        "http://127.0.0.1:5000",
        "https://magolis.onrender.com"
    ],
    async_mode='threading',
    ping_timeout=60,
    ping_interval=25
)

logger.info("SocketIO running in threading mode")

# ==================== DATABASE SETUP (FIXED - NO CONNECTION POOL) ====================

DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://lmsdatabase_8ag3_user:6WD9lOnHkiU7utlUUjT88m4XgEYQMTLb@dpg-ctp9h0aj1k6c739h9di0-a.oregon-postgres.render.com/lmsdatabase_8ag3')

def get_db_connection():
    """Create a NEW database connection each time - FIXES the connection pool issue"""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        return conn
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        raise

@contextmanager
def get_db_cursor(commit=True):
    """Get a database cursor with automatic cleanup - FIXED version"""
    conn = None
    cursor = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        yield cursor
        if commit:
            conn.commit()
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Database error: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def init_db():
    """Initialize database tables"""
    with get_db_cursor(commit=True) as cursor:
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
        
        logger.info("Database initialized successfully")

# Initialize database
init_db()

# ==================== HELPER FUNCTIONS FOR DATABASE OPERATIONS ====================

def save_contact(platform, platform_user_id, display_name=None, phone_number=None, opt_in=True):
    """Save or update contact - FIXED to use new cursor pattern"""
    with get_db_cursor(commit=True) as cursor:
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
        return result['id']

def save_message(contact_id, platform, direction, message, status='sent'):
    """Save a message - FIXED to use new cursor pattern"""
    with get_db_cursor(commit=True) as cursor:
        cursor.execute("""
            INSERT INTO messages (contact_id, platform, direction, message, status, sent_at)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (contact_id, platform, direction, message, status, datetime.now()))

def get_recipients_for_broadcast(platform, audience_filter='all', tags=None):
    """Get recipients for broadcast - FIXED to use new cursor pattern"""
    with get_db_cursor(commit=False) as cursor:
        query = "SELECT id, platform_user_id, display_name FROM contacts WHERE platform = %s AND opt_in = TRUE"
        params = [platform]

        if audience_filter == 'active':
            query += " AND last_interaction >= (CURRENT_DATE - INTERVAL '30 days')"
        elif audience_filter == 'tagged' and tags:
            tag_list = [t.strip() for t in tags.split(',') if t.strip()]
            if tag_list:
                # Assuming tags are stored as comma-separated in a 'tags' column
                tag_conditions = " OR ".join(["tags ILIKE %s" for _ in tag_list])
                query += f" AND (" + tag_conditions + ")"
                params.extend([f"%{tag}%" for tag in tag_list])

        cursor.execute(query, tuple(params))
        return [dict(row) for row in cursor.fetchall()]

def create_broadcast_record(user_id, name, platform, message, audience_filter, total_recipients):
    """Create a broadcast record - returns broadcast_id"""
    with get_db_cursor(commit=True) as cursor:
        cursor.execute('''
            INSERT INTO broadcasts (user_id, name, platform, message, audience_filter, total_recipients, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        ''', (user_id, name, platform, message, audience_filter, total_recipients, 'processing'))
        return cursor.fetchone()['id']

def update_broadcast_stats(broadcast_id, sent_count, failed_count):
    """Update broadcast stats"""
    with get_db_cursor(commit=True) as cursor:
        cursor.execute('''
            UPDATE broadcasts 
            SET sent_count = %s, failed_count = %s
            WHERE id = %s
        ''', (sent_count, failed_count, broadcast_id))

def complete_broadcast(broadcast_id):
    """Mark broadcast as completed"""
    with get_db_cursor(commit=True) as cursor:
        cursor.execute('''
            UPDATE broadcasts 
            SET status = 'completed', completed_at = %s
            WHERE id = %s
        ''', (datetime.now(), broadcast_id))

def add_broadcast_recipient(broadcast_id, contact_id, status, error_message=None):
    """Add broadcast recipient record"""
    with get_db_cursor(commit=True) as cursor:
        cursor.execute('''
            INSERT INTO broadcast_recipients (broadcast_id, contact_id, status, error_message, sent_at)
            VALUES (%s, %s, %s, %s, %s)
        ''', (broadcast_id, contact_id, status, error_message, datetime.now()))

def get_all_contacts(platform=None, opt_in_only=False, search=None):
    """Get contacts for display"""
    with get_db_cursor(commit=False) as cursor:
        query = "SELECT * FROM contacts WHERE 1=1"
        params = []
        if platform:
            query += " AND platform = %s"
            params.append(platform)
        if opt_in_only:
            query += " AND opt_in = TRUE"
        if search:
            query += " AND (display_name ILIKE %s OR platform_user_id ILIKE %s)"
            params.extend([f"%{search}%", f"%{search}%"])
        query += " ORDER BY last_interaction DESC NULLS LAST LIMIT 200"
        cursor.execute(query, tuple(params))
        return [dict(row) for row in cursor.fetchall()]

def get_messages(limit=50, platform=None):
    """Get message history"""
    with get_db_cursor(commit=False) as cursor:
        cursor.execute('''
            SELECT m.*, c.display_name, c.platform_user_id
            FROM messages m
            JOIN contacts c ON m.contact_id = c.id
            ORDER BY m.sent_at DESC LIMIT %s
        ''', (limit,))
        return [dict(row) for row in cursor.fetchall()]

def get_broadcasts():
    """Get broadcast history"""
    with get_db_cursor(commit=False) as cursor:
        cursor.execute('''
            SELECT * FROM broadcasts 
            ORDER BY created_at DESC 
            LIMIT 20
        ''')
        return [dict(row) for row in cursor.fetchall()]

def get_dashboard_stats():
    """Get dashboard statistics"""
    with get_db_cursor(commit=False) as cursor:
        cursor.execute('SELECT COUNT(*) as total, SUM(CASE WHEN opt_in = TRUE THEN 1 ELSE 0 END) as opted_in FROM contacts')
        contact_stats = cursor.fetchone()
        
        cursor.execute('SELECT platform, COUNT(*) as count FROM contacts GROUP BY platform')
        contacts_by_platform = cursor.fetchall()
        
        cursor.execute('SELECT COUNT(*) as total, SUM(CASE WHEN direction = \'outgoing\' THEN 1 ELSE 0 END) as sent FROM messages')
        message_stats = cursor.fetchone()
        
        cursor.execute('SELECT COUNT(*) as total FROM broadcasts')
        broadcast_stats = cursor.fetchone()
        
        return {
            'total_contacts': contact_stats['total'] if contact_stats else 0,
            'opted_in_contacts': contact_stats['opted_in'] if contact_stats else 0,
            'sent_messages': message_stats['sent'] if message_stats else 0,
            'total_broadcasts': broadcast_stats['total'] if broadcast_stats else 0,
            'contacts_by_platform': [{'platform': row['platform'], 'count': row['count']} for row in contacts_by_platform] if contacts_by_platform else []
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
        self.verify_token = os.getenv('WHATSAPP_VERIFY_TOKEN', 'magolis_whatsapp_verify')
        self.is_configured = bool(self.access_token and self.phone_number_id)
        self.init_error = None
        if not self.access_token:
            self.init_error = 'WHATSAPP_ACCESS_TOKEN env var is not set'
        elif not self.phone_number_id:
            self.init_error = 'WHATSAPP_PHONE_ID env var is not set'

    def _headers(self):
        return {"Authorization": f"Bearer {self.access_token}", "Content-Type": "application/json"}

    def send_message(self, recipient_id, content):
        if not self.is_configured:
            return {'success': False, 'error': self.init_error or 'WhatsApp not configured'}

        # Normalise to E.164 without leading +  (WhatsApp API expects digits only or full E.164)
        to = recipient_id.lstrip('+')
        url = f"https://graph.facebook.com/v18.0/{self.phone_number_id}/messages"
        payload = {
            "messaging_product": "whatsapp",
            "to": to,
            "type": "text",
            "text": {"body": content}
        }
        try:
            response = requests.post(url, json=payload, headers=self._headers(), timeout=30)
            if response.status_code == 200:
                data = response.json()
                # API returns 200 even on some errors; check messages array
                if data.get('messages'):
                    return {'success': True, 'platform': 'whatsapp', 'message_id': data['messages'][0].get('id')}
            error_data = response.json()
            error_msg = error_data.get('error', {}).get('message', f'HTTP {response.status_code}')
            error_code = error_data.get('error', {}).get('code')
            logger.error(f"WhatsApp send failed (code {error_code}): {error_msg} | to: {to}")
            return {'success': False, 'error': error_msg}
        except Exception as e:
            logger.error(f"WhatsApp send_message exception: {e}")
            return {'success': False, 'error': str(e)}

    def send_template(self, recipient_id, template_name, language_code='en_US'):
        """Send an approved template message (works outside 24h window)"""
        if not self.is_configured:
            return {'success': False, 'error': self.init_error or 'WhatsApp not configured'}
        to = recipient_id.lstrip('+')
        url = f"https://graph.facebook.com/v18.0/{self.phone_number_id}/messages"
        payload = {
            "messaging_product": "whatsapp",
            "to": to,
            "type": "template",
            "template": {"name": template_name, "language": {"code": language_code}}
        }
        try:
            response = requests.post(url, json=payload, headers=self._headers(), timeout=30)
            data = response.json()
            if response.status_code == 200 and data.get('messages'):
                return {'success': True, 'platform': 'whatsapp'}
            error_msg = data.get('error', {}).get('message', f'HTTP {response.status_code}')
            return {'success': False, 'error': error_msg}
        except Exception as e:
            return {'success': False, 'error': str(e)}

    def diagnose(self):
        """Check token validity and phone number registration"""
        result = {
            'access_token_set': bool(self.access_token),
            'phone_number_id_set': bool(self.phone_number_id),
            'is_configured': self.is_configured,
        }
        if not self.is_configured:
            result['error'] = self.init_error
            return result
        try:
            r = requests.get(
                f"https://graph.facebook.com/v18.0/{self.phone_number_id}",
                params={'access_token': self.access_token, 'fields': 'verified_name,display_phone_number,quality_rating,status'},
                timeout=10
            )
            data = r.json()
            if 'error' in data:
                result['api_error'] = data['error'].get('message')
                result['api_ok'] = False
            else:
                result['display_phone_number'] = data.get('display_phone_number')
                result['verified_name'] = data.get('verified_name')
                result['quality_rating'] = data.get('quality_rating')
                result['status'] = data.get('status')
                result['api_ok'] = True
        except Exception as e:
            result['api_error'] = str(e)
        return result

    def get_conversations(self, limit=50):
        """WhatsApp Cloud API does not support listing conversations — return contacts from DB"""
        return {'success': False, 'error': 'WhatsApp contacts are added automatically when they message you via webhook. Use CSV import or add manually.'}


class FacebookAdapter:
    def __init__(self):
        self.page_access_token = os.getenv('FACEBOOK_PAGE_TOKEN')
        self.page_id = os.getenv('FACEBOOK_PAGE_ID')
        self.is_configured = bool(self.page_access_token and self.page_id)
    
    def get_all_conversations(self, limit=200):
        """Fetch ALL historical conversations - including those before webhook setup"""
        if not self.is_configured:
            return {'success': False, 'error': 'Facebook not configured'}
        
        page_id = self.page_id
        all_conversations = []
        url = f"https://graph.facebook.com/v18.0/{page_id}/conversations"
        params = {
            'access_token': self.page_access_token,
            'fields': 'participants,updated_time,message_count,messages.limit(5){message,created_time,from}',
            'limit': limit
        }
        
        try:
            while url:
                response = requests.get(url, params=params)
                data = response.json()
                
                if 'error' in data:
                    logger.error(f"Facebook API error: {data['error']}")
                    return {'success': False, 'error': data['error']['message']}
                
                for conv in data.get('data', []):
                    participants = conv.get('participants', {}).get('data', [])
                    user_participant = None
                    for p in participants:
                        if p.get('id') != page_id:
                            user_participant = p
                            break
                    
                    if user_participant:
                        messages_data = conv.get('messages', {}).get('data', [])
                        messages = []
                        for msg in messages_data:
                            messages.append({
                                'text': msg.get('message', ''),
                                'created_time': msg.get('created_time'),
                                'from_id': msg.get('from', {}).get('id')
                            })
                        
                        all_conversations.append({
                            'psid': user_participant['id'],
                            'name': user_participant.get('name', 'Facebook User'),
                            'last_message': messages[0].get('text', '') if messages else None,
                            'last_interaction': conv.get('updated_time'),
                            'message_count': conv.get('message_count', 0),
                            'messages': messages
                        })
                
                # Handle pagination - get next page
                url = data.get('paging', {}).get('next')
                params = None  # Next URL already has token
            
            logger.info(f"Fetched {len(all_conversations)} historical conversations")
            return {'success': True, 'conversations': all_conversations}
            
        except Exception as e:
            logger.error(f"Facebook API error: {str(e)}")
            return {'success': False, 'error': str(e)}
    
    def get_conversation_with_user(self, psid, limit=50):
        """Get full conversation history with a specific user"""
        if not self.is_configured:
            return {'success': False, 'error': 'Facebook not configured'}
        
        page_id = self.page_id
        
        try:
            # First get the conversation ID
            conv_url = f"https://graph.facebook.com/v18.0/{page_id}/conversations"
            params = {
                'access_token': self.page_access_token,
                'filter': 'participants',
                'user_id': psid,
                'fields': 'id'
            }
            
            response = requests.get(conv_url, params=params)
            data = response.json()
            
            if not data.get('data'):
                return {'success': True, 'messages': []}
            
            conversation_id = data['data'][0]['id']
            
            # Get all messages from that conversation
            messages_url = f"https://graph.facebook.com/v18.0/{conversation_id}/messages"
            msg_params = {
                'access_token': self.page_access_token,
                'fields': 'message,created_time,from,id,attachments',
                'limit': limit
            }
            
            all_messages = []
            url = messages_url
            
            while url:
                msg_response = requests.get(url, params=msg_params if url == messages_url else None)
                messages_data = msg_response.json()
                
                for msg in messages_data.get('data', []):
                    all_messages.append({
                        'id': msg.get('id'),
                        'content': msg.get('message', ''),
                        'timestamp': msg.get('created_time'),
                        'direction': 'incoming' if msg.get('from', {}).get('id') != page_id else 'outgoing',
                        'sender_name': msg.get('from', {}).get('name', 'Unknown'),
                        'sender_id': msg.get('from', {}).get('id')
                    })
                
                url = messages_data.get('paging', {}).get('next')
                msg_params = None
            
            return {'success': True, 'messages': all_messages, 'count': len(all_messages)}
            
        except Exception as e:
            logger.error(f"Error fetching conversation: {str(e)}")
            return {'success': False, 'error': str(e)}
        
class TwitterAdapter:
    def __init__(self):
        self.bearer_token = os.getenv('TWITTER_BEARER_TOKEN')
        self.api_key = os.getenv('TWITTER_API_KEY')
        self.api_secret = os.getenv('TWITTER_API_SECRET')
        self.access_token = os.getenv('TWITTER_ACCESS_TOKEN')
        self.access_secret = os.getenv('TWITTER_ACCESS_SECRET')
        self.is_configured = bool(self.bearer_token) and tweepy is not None
        self.client = None
        
        if self.is_configured and tweepy:
            try:
                self.client = tweepy.Client(
                    bearer_token=self.bearer_token,
                    consumer_key=self.api_key,
                    consumer_secret=self.api_secret,
                    access_token=self.access_token,
                    access_token_secret=self.access_secret
                )
            except Exception as e:
                logger.error(f"Twitter init error: {e}")
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

    def diagnose(self):
        result = {
            'bearer_token_set': bool(self.bearer_token),
            'is_configured': self.is_configured,
        }
        if not self.bearer_token:
            result['error'] = 'TWITTER_BEARER_TOKEN env var is not set'
            return result
        if not self.client:
            result['error'] = 'Twitter client failed to initialise (tweepy not installed or bad token)'
            result['api_ok'] = False
            return result
        try:
            me = self.client.get_me()
            if me and me.data:
                result['username'] = me.data.username
                result['name']     = me.data.name
                result['api_ok']   = True
            else:
                result['api_error'] = 'No user data returned'
                result['api_ok']    = False
        except Exception as e:
            result['api_error'] = str(e)
            result['api_ok']    = False
        return result


class InstagramAdapter:
    def __init__(self):
        self.access_token = os.getenv('FACEBOOK_PAGE_TOKEN')
        self.page_id = os.getenv('FACEBOOK_PAGE_ID')
        self.instagram_business_id = os.getenv('INSTAGRAM_BUSINESS_ID')
        self.init_error = None

        if not self.access_token:
            self.is_configured = False
            self.init_error = 'FACEBOOK_PAGE_TOKEN env var is not set'
        elif not self.page_id:
            self.is_configured = False
            self.init_error = 'FACEBOOK_PAGE_ID env var is not set'
        else:
            self.is_configured = True
            if self.instagram_business_id:
                logger.info(f"Instagram Business ID loaded from env var: {self.instagram_business_id}")
            else:
                self._cache_business_id()
    
    def _cache_business_id(self):
        """Get and cache Instagram Business Account ID - tries multiple methods"""
        not_linked_codes = set()

        # Method 1: instagram_business_account (Business accounts)
        try:
            r = requests.get(
                f"https://graph.facebook.com/v18.0/{self.page_id}",
                params={'access_token': self.access_token, 'fields': 'instagram_business_account'}
            )
            data = r.json()
            if 'instagram_business_account' in data:
                self.instagram_business_id = data['instagram_business_account']['id']
                logger.info(f"Instagram Business ID loaded (method 1): {self.instagram_business_id}")
                return
            if 'error' in data:
                not_linked_codes.add(data['error'].get('code'))
                logger.warning(f"Instagram method 1 failed (code {data['error'].get('code')}): {data['error'].get('message')} — trying method 2")
        except Exception as e:
            logger.warning(f"Instagram method 1 exception: {e}")

        # Method 2: connected_instagram_account (Creator accounts)
        try:
            r = requests.get(
                f"https://graph.facebook.com/v18.0/{self.page_id}",
                params={'access_token': self.access_token, 'fields': 'connected_instagram_account'}
            )
            data = r.json()
            if 'connected_instagram_account' in data:
                self.instagram_business_id = data['connected_instagram_account']['id']
                logger.info(f"Instagram Business ID loaded (method 2): {self.instagram_business_id}")
                return
            if 'error' in data:
                not_linked_codes.add(data['error'].get('code'))
                logger.warning(f"Instagram method 2 failed (code {data['error'].get('code')}): {data['error'].get('message')} — trying method 3")
        except Exception as e:
            logger.warning(f"Instagram method 2 exception: {e}")

        # Method 3: instagram_accounts field (works for some token types)
        try:
            r = requests.get(
                f"https://graph.facebook.com/v18.0/{self.page_id}",
                params={'access_token': self.access_token, 'fields': 'instagram_accounts{id,name,username}'}
            )
            data = r.json()
            accounts = data.get('instagram_accounts', {}).get('data', [])
            if accounts:
                self.instagram_business_id = accounts[0]['id']
                logger.info(f"Instagram Business ID loaded (method 3): {self.instagram_business_id}")
                return
            if 'error' in data:
                not_linked_codes.add(data['error'].get('code'))
                logger.warning(f"Instagram method 3 failed: {data['error'].get('message')} — trying method 4")
        except Exception as e:
            logger.warning(f"Instagram method 3 exception: {e}")

        # Method 4: /me/accounts (User Access Token path)
        try:
            r = requests.get(
                "https://graph.facebook.com/v18.0/me/accounts",
                params={'access_token': self.access_token, 'fields': 'id,name,instagram_business_account,connected_instagram_account'}
            )
            data = r.json()
            for page in data.get('data', []):
                if page.get('id') == self.page_id:
                    ig = page.get('instagram_business_account') or page.get('connected_instagram_account')
                    if ig:
                        self.instagram_business_id = ig['id']
                        logger.info(f"Instagram Business ID loaded (method 4): {self.instagram_business_id}")
                        return
        except Exception as e:
            logger.warning(f"Instagram method 4 exception: {e}")

        # All methods failed — determine most likely cause
        # Code 100 = field doesn't exist on this object = Instagram not linked to page
        if 100 in not_linked_codes:
            self.init_error = (
                'Instagram account is NOT linked to your Facebook Page. '
                'Fix: Go to your Facebook Page > Settings > Linked Accounts (or Instagram section) > Connect your Instagram account. '
                'Your Instagram must be a Business or Creator account (not Personal).'
            )
        else:
            self.init_error = (
                'Could not retrieve Instagram Business Account ID. '
                'Ensure your token has instagram_basic + instagram_manage_messages permissions, '
                'your Instagram is a Business/Creator account, and it is linked to your Facebook Page.'
            )
        self.is_configured = False
        logger.error(f"Instagram Business ID could not be loaded: {self.init_error}")
    
    def get_conversations(self, limit=50):
        if not self.is_configured or not self.instagram_business_id:
            return {'success': False, 'error': self.init_error or 'Instagram not configured - no Business Account'}
        
        url = f"https://graph.facebook.com/v18.0/{self.instagram_business_id}/conversations"
        params = {
            'access_token': self.access_token,
            'fields': 'participants,updated_time,messages.limit(1){text,created_time,from}',
            'limit': limit
        }
        
        try:
            response = requests.get(url, params=params)
            data = response.json()
            
            if 'error' in data:
                return {'success': False, 'error': data['error'].get('message')}
            
            conversations = []
            for conv in data.get('data', []):
                participants = conv.get('participants', {}).get('data', [])
                user_participant = None
                
                for p in participants:
                    if p.get('id') != self.instagram_business_id and p.get('id') != self.page_id:
                        user_participant = p
                        break
                
                if user_participant:
                    messages_data = conv.get('messages', {}).get('data', [])
                    last_message = messages_data[0] if messages_data else None
                    
                    conversations.append({
                        'psid': user_participant['id'],
                        'name': user_participant.get('name', 'Instagram User'),
                        'last_message': last_message.get('text', '') if last_message else None,
                        'last_interaction': conv.get('updated_time')
                    })
            
            return {'success': True, 'conversations': conversations}
            
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def get_conversation_history(self, psid, limit=100):
        if not self.is_configured or not self.instagram_business_id:
            return []
        
        url = f"https://graph.facebook.com/v18.0/{self.instagram_business_id}/conversations"
        params = {
            'access_token': self.access_token,
            'fields': f'messages{{text,created_time,from,id}}',
            'user_id': psid,
            'limit': limit
        }
        
        try:
            response = requests.get(url, params=params)
            data = response.json()
            
            messages = []
            for conv in data.get('data', []):
                for msg in conv.get('messages', {}).get('data', []):
                    messages.append({
                        'id': msg.get('id'),
                        'content': msg.get('text', ''),
                        'timestamp': msg.get('created_time'),
                        'direction': 'incoming' if msg.get('from', {}).get('id') != self.instagram_business_id else 'outgoing'
                    })
            
            return sorted(messages, key=lambda x: x['timestamp'])
            
        except Exception as e:
            logger.error(f"Conversation history error: {e}")
            return []
    
    def send_message(self, recipient_id, content):
        if not self.is_configured:
            return {'success': False, 'error': 'Instagram not configured'}
        
        url = f"https://graph.facebook.com/v18.0/{self.page_id}/messages"
        payload = {
            "recipient": {"id": recipient_id},
            "message": {"text": content},
            "messaging_type": "RESPONSE"
        }
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }
        
        try:
            response = requests.post(url, json=payload, headers=headers, timeout=30)
            if response.status_code == 200:
                return {'success': True, 'platform': 'instagram'}
            return {'success': False, 'error': f'HTTP {response.status_code}: {response.text}'}
        except Exception as e:
            return {'success': False, 'error': str(e)}


class LinkedInAdapter:
    """
    LinkedIn API v2 — for posting to the member's feed.
    NOTE: LinkedIn has no public DM API. 'Broadcasting' = publishing a post/share.
    Contacts must be added manually or via CSV.
    Requires: LINKEDIN_ACCESS_TOKEN (OAuth 2.0 token)
    Token needs scopes: r_liteprofile, w_member_social
    """
    BASE = 'https://api.linkedin.com/v2'

    def __init__(self):
        self.access_token  = os.getenv('LINKEDIN_ACCESS_TOKEN')
        self.is_configured = bool(self.access_token)
        self.init_error    = None
        self._person_urn   = None  # cached urn:li:person:{id}
        if not self.access_token:
            self.init_error = 'LINKEDIN_ACCESS_TOKEN env var is not set'

    def _headers(self):
        return {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json',
            'X-Restli-Protocol-Version': '2.0.0'
        }

    def _get_person_urn(self):
        if self._person_urn:
            return self._person_urn
        try:
            r = requests.get(f'{self.BASE}/me', headers=self._headers(), timeout=10)
            data = r.json()
            if 'id' in data:
                self._person_urn = f'urn:li:person:{data["id"]}'
        except Exception:
            pass
        return self._person_urn

    def diagnose(self):
        result = {
            'access_token_set': bool(self.access_token),
            'is_configured': self.is_configured,
        }
        if not self.is_configured:
            result['error'] = self.init_error
            return result
        try:
            r = requests.get(
                f'{self.BASE}/me',
                headers=self._headers(),
                params={'fields': 'id,firstName,lastName,vanityName'},
                timeout=10
            )
            data = r.json()
            if r.status_code >= 400 or 'serviceErrorCode' in data:
                result['api_error'] = data.get('message', f'HTTP {r.status_code}')
                result['api_ok'] = False
            else:
                first = list((data.get('firstName', {}).get('localized') or {}).values() or [''])[0]
                last  = list((data.get('lastName',  {}).get('localized') or {}).values() or [''])[0]
                result['name']   = f'{first} {last}'.strip()
                result['vanity'] = data.get('vanityName')
                result['api_ok'] = True
        except Exception as e:
            result['api_error'] = str(e)
            result['api_ok'] = False
        return result

    def publish_post(self, text):
        """Publish a text post to the member's LinkedIn feed."""
        if not self.is_configured:
            return {'success': False, 'error': self.init_error}
        try:
            person_urn = self._get_person_urn()
            if not person_urn:
                return {'success': False, 'error': 'Could not retrieve LinkedIn person URN — check token'}
            payload = {
                'author': person_urn,
                'lifecycleState': 'PUBLISHED',
                'specificContent': {
                    'com.linkedin.ugc.ShareContent': {
                        'shareCommentary': {'text': text},
                        'shareMediaCategory': 'NONE'
                    }
                },
                'visibility': {
                    'com.linkedin.ugc.MemberNetworkVisibility': 'PUBLIC'
                }
            }
            r = requests.post(f'{self.BASE}/ugcPosts', headers=self._headers(), json=payload, timeout=30)
            if r.status_code in (200, 201):
                post_id = r.headers.get('X-RestLi-Id') or (r.json().get('id') if r.content else None)
                return {'success': True, 'post_id': post_id, 'platform': 'linkedin'}
            data = r.json()
            return {'success': False, 'error': data.get('message', f'HTTP {r.status_code}')}
        except Exception as e:
            return {'success': False, 'error': str(e)}

    def send_message(self, recipient_id, content):
        """Broadcast compatibility — publishes as a LinkedIn post (no DM API)."""
        return self.publish_post(content)


# Initialize adapters
adapters = {
    'whatsapp': WhatsAppAdapter(),
    'facebook': FacebookAdapter(),
    'twitter': TwitterAdapter(),
    'instagram': InstagramAdapter(),
    'linkedin': LinkedInAdapter()
}

# Facebook historical sync guard to avoid expensive API calls on every request.
_facebook_backfill_lock = threading.Lock()
_facebook_last_backfill = datetime.min

def maybe_backfill_facebook_contacts(force=False):
    """Backfill older Facebook contacts from conversation history with cooldown control."""
    global _facebook_last_backfill

    fb = adapters.get('facebook')
    if not fb or not fb.is_configured:
        return {'success': False, 'skipped': True, 'reason': 'facebook-not-configured'}

    cooldown_seconds = int(os.getenv('FACEBOOK_SYNC_COOLDOWN_SECONDS', '900'))
    now = datetime.now()

    with _facebook_backfill_lock:
        if not force and (now - _facebook_last_backfill).total_seconds() < cooldown_seconds:
            return {'success': True, 'skipped': True, 'reason': 'cooldown'}

        result = fb.get_all_conversations(limit=200)
        if not result.get('success'):
            return {'success': False, 'error': result.get('error', 'Unknown Facebook sync error')}

        synced_count = 0
        for conv in result.get('conversations', []):
            save_contact(
                platform='facebook',
                platform_user_id=conv.get('psid'),
                display_name=conv.get('name'),
                opt_in=True
            )
            synced_count += 1

        _facebook_last_backfill = datetime.now()
        return {
            'success': True,
            'skipped': False,
            'synced': synced_count,
            'conversations': result.get('conversations', [])
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
    
    with get_db_cursor(commit=False) as cursor:
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

# ==================== TWITTER ROUTES ====================

@app.route('/api/twitter/diagnose', methods=['GET'])
@login_required
def twitter_diagnose():
    return jsonify(adapters['twitter'].diagnose())


@app.route('/api/twitter/sync-contacts', methods=['POST'])
@login_required
def sync_twitter_contacts():
    """Return contacts with platform='twitter' from DB (Twitter DM contacts API requires Elevated access)."""
    try:
        with get_db_cursor(commit=False) as cursor:
            cursor.execute("""
                SELECT id, platform_user_id, display_name, opt_in
                FROM contacts WHERE platform = 'twitter' ORDER BY created_at DESC LIMIT 100
            """)
            rows = cursor.fetchall()
        contacts = [{'id': r[0], 'platform_user_id': r[1], 'display_name': r[2], 'opt_in': r[3]} for r in rows]
        return jsonify({'success': True, 'contacts': contacts})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ==================== LINKEDIN ROUTES ====================

@app.route('/api/linkedin/diagnose', methods=['GET'])
@login_required
def linkedin_diagnose():
    return jsonify(adapters['linkedin'].diagnose())


@app.route('/api/linkedin/sync-contacts', methods=['POST'])
@login_required
def sync_linkedin_contacts():
    """Return contacts with platform='linkedin' from DB (LinkedIn has no public contacts API)."""
    try:
        with get_db_cursor(commit=False) as cursor:
            cursor.execute("""
                SELECT id, platform_user_id, display_name, opt_in
                FROM contacts WHERE platform = 'linkedin' ORDER BY created_at DESC LIMIT 100
            """)
            rows = cursor.fetchall()
        contacts = [{'id': r[0], 'platform_user_id': r[1], 'display_name': r[2], 'opt_in': r[3]} for r in rows]
        return jsonify({'success': True, 'contacts': contacts})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/linkedin/publish', methods=['POST'])
@login_required
def publish_linkedin_post():
    """Publish a post to LinkedIn (broadcast equivalent)."""
    data = request.json
    text = (data.get('text') or '').strip()
    if not text:
        return jsonify({'success': False, 'error': 'Text is required'}), 400
    result = adapters['linkedin'].publish_post(text)
    if result.get('success'):
        socketio.emit('message_sent', {
            'platform': 'linkedin',
            'content': text[:100],
            'timestamp': datetime.now().isoformat()
        })
    return jsonify(result)


# ==================== WHATSAPP ROUTES ====================

@app.route('/api/whatsapp/diagnose', methods=['GET'])
@login_required
def whatsapp_diagnose():
    result = adapters['whatsapp'].diagnose()
    return jsonify(result)


@app.route('/webhook/whatsapp', methods=['GET', 'POST'])
def whatsapp_webhook():
    """WhatsApp Cloud API webhook — verification + incoming message handler"""
    if request.method == 'GET':
        mode      = request.args.get('hub.mode')
        token     = request.args.get('hub.verify_token')
        challenge = request.args.get('hub.challenge')
        expected  = os.getenv('WHATSAPP_VERIFY_TOKEN', 'magolis_whatsapp_verify')
        if mode == 'subscribe' and token == expected:
            logger.info('WhatsApp webhook verified')
            return challenge, 200
        logger.error(f'WhatsApp webhook verification failed. Got: {token}')
        return 'Forbidden', 403

    try:
        payload = request.json
        if not payload:
            return jsonify({'status': 'ok'}), 200

        for entry in payload.get('entry', []):
            for change in entry.get('changes', []):
                value = change.get('value', {})
                messages = value.get('messages', [])
                contacts_meta = {c['wa_id']: c.get('profile', {}).get('name', 'WhatsApp User')
                                 for c in value.get('contacts', [])}

                for msg in messages:
                    sender_wa_id = msg.get('from')          # phone number digits
                    msg_type     = msg.get('type', 'text')
                    content      = ''

                    if msg_type == 'text':
                        content = msg.get('text', {}).get('body', '')
                    elif msg_type == 'image':
                        content = '[Image]'
                    elif msg_type == 'audio':
                        content = '[Audio]'
                    elif msg_type == 'document':
                        content = '[Document]'
                    elif msg_type == 'location':
                        loc = msg.get('location', {})
                        content = f"[Location: {loc.get('latitude')},{loc.get('longitude')}]"
                    else:
                        content = f'[{msg_type}]'

                    display_name = contacts_meta.get(sender_wa_id, 'WhatsApp User')
                    contact_id = save_contact(
                        platform='whatsapp',
                        platform_user_id=sender_wa_id,
                        display_name=display_name,
                        phone_number=f'+{sender_wa_id}',
                        opt_in=True
                    )
                    if content:
                        save_message(contact_id, 'whatsapp', 'incoming', content)

                    socketio.emit('new_message', {
                        'platform': 'whatsapp',
                        'sender_id': sender_wa_id,
                        'display_name': display_name,
                        'content': content,
                        'timestamp': datetime.now().isoformat()
                    })
                    logger.info(f'WhatsApp message from {sender_wa_id} ({display_name}): {content[:80]}')

        return jsonify({'status': 'ok'}), 200
    except Exception as e:
        logger.error(f'WhatsApp webhook error: {e}')
        return jsonify({'status': 'error'}), 500


@app.route('/api/whatsapp/sync-contacts', methods=['POST'])
@login_required
def sync_whatsapp_contacts():
    """Load WhatsApp contacts already saved in the database (added via webhook)"""
    try:
        with get_db_cursor(commit=False) as cursor:
            cursor.execute('''
                SELECT id, platform_user_id, display_name, phone_number, opt_in, last_interaction
                FROM contacts
                WHERE platform = 'whatsapp'
                ORDER BY last_interaction DESC NULLS LAST
                LIMIT 200
            ''')
            rows = cursor.fetchall()

        contacts = [dict(r) for r in rows]
        return jsonify({'success': True, 'contacts': contacts, 'count': len(contacts)})
    except Exception as e:
        logger.error(f'WhatsApp sync-contacts error: {e}')
        return jsonify({'success': False, 'error': str(e)}), 500


# ==================== FACEBOOK CONTACT SYNC ROUTES ====================

@app.route('/api/facebook/diagnose', methods=['GET'])
@login_required
def facebook_diagnose():
    """Check Facebook token validity and permissions"""
    page_token = os.getenv('FACEBOOK_PAGE_TOKEN')
    page_id = os.getenv('FACEBOOK_PAGE_ID')

    if not page_token:
        return jsonify({'success': False, 'error': 'FACEBOOK_PAGE_TOKEN not set'})
    if not page_id:
        return jsonify({'success': False, 'error': 'FACEBOOK_PAGE_ID not set'})

    result = {'page_token_set': True, 'page_id': page_id}

    # Check token info
    try:
        r = requests.get(
            'https://graph.facebook.com/debug_token',
            params={
                'input_token': page_token,
                'access_token': page_token
            }
        )
        token_data = r.json().get('data', {})
        result['token_valid'] = token_data.get('is_valid', False)
        result['token_expires_at'] = token_data.get('expires_at', 'unknown')
        result['token_type'] = token_data.get('type', 'unknown')
        result['token_scopes'] = token_data.get('scopes', [])
        result['token_error'] = token_data.get('error', {}).get('message') if not token_data.get('is_valid') else None
    except Exception as e:
        result['token_check_error'] = str(e)

    # Try a simple API call to confirm the token works
    try:
        r2 = requests.get(
            f'https://graph.facebook.com/v18.0/{page_id}',
            params={'access_token': page_token, 'fields': 'id,name'}
        )
        page_data = r2.json()
        if 'error' in page_data:
            result['page_api_error'] = page_data['error'].get('message')
        else:
            result['page_name'] = page_data.get('name')
            result['page_api_ok'] = True
    except Exception as e:
        result['page_api_error'] = str(e)

    # Send a test message to yourself (skip — just check token)
    result['recommendation'] = []
    if not result.get('token_valid'):
        result['recommendation'].append('Your token is INVALID or EXPIRED. Generate a new permanent Page Access Token in Meta Business Suite > Settings > Page Access Tokens.')
    if 'pages_messaging' not in result.get('token_scopes', []):
        result['recommendation'].append('Token is missing "pages_messaging" permission. Regenerate with this scope.')
    if not result.get('recommendation'):
        result['recommendation'].append('Token looks valid. If messages still fail, check server logs for the exact Facebook API error per recipient.')

    return jsonify(result)


@app.route('/api/facebook/sync-contacts', methods=['POST'])
@login_required
def sync_facebook_contacts():
    """Sync ALL historical Facebook contacts - including those who messaged you long ago"""
    try:
        if not adapters['facebook'].is_configured:
            return jsonify({'success': False, 'error': 'Facebook not configured'}), 400

        result = maybe_backfill_facebook_contacts(force=True)
        if not result.get('success'):
            return jsonify({'success': False, 'error': result.get('error', 'Facebook sync failed')}), 400

        new_contacts = []
        for conv in result.get('conversations', []):
            new_contacts.append({
                'psid': conv.get('psid'),
                'name': conv.get('name'),
                'last_message': conv.get('last_message'),
                'last_interaction': conv.get('last_interaction'),
                'message_count': conv.get('message_count', 0)
            })
        
        return jsonify({
            'success': True,
            'synced': result.get('synced', 0),
            'contacts': new_contacts,
            'message': f"Successfully synced {result.get('synced', 0)} historical Facebook contacts"
        })
        
    except Exception as e:
        logger.error(f"Sync error: {str(e)}")
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
            return jsonify({'success': True, 'messages': []})
        
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
        
        return jsonify({'success': True, 'messages': messages})
        
    except Exception as e:
        logger.error(f"Error fetching conversation: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/instagram/diagnose', methods=['GET'])
@login_required
def instagram_diagnose():
    """Diagnose Instagram configuration issues"""
    adapter = adapters['instagram']
    page_token = os.getenv('FACEBOOK_PAGE_TOKEN')
    page_id = os.getenv('FACEBOOK_PAGE_ID')
    return jsonify({
        'FACEBOOK_PAGE_TOKEN_set': bool(page_token),
        'FACEBOOK_PAGE_ID_set': bool(page_id),
        'FACEBOOK_PAGE_ID': page_id,
        'is_configured': adapter.is_configured,
        'instagram_business_id': adapter.instagram_business_id,
        'init_error': getattr(adapter, 'init_error', None)
    })


@app.route('/api/instagram/sync-contacts', methods=['POST'])
@login_required
def sync_instagram_contacts():
    try:
        instagram_adapter = adapters['instagram']
        
        if not instagram_adapter.is_configured:
            return jsonify({'success': False, 'error': 'Instagram not configured'}), 400
        
        result = instagram_adapter.get_conversations(limit=100)
        
        # Error #3 = app not approved for instagram_manage_messages — fall back to DB contacts
        if not result['success']:
            err = result.get('error', '')
            is_permission_error = '(#3)' in err or 'capability' in err.lower() or 'permission' in err.lower()
            if is_permission_error:
                logger.warning(f"Instagram Conversations API not available ({err}) — falling back to DB contacts")
                db_contacts = get_all_contacts(platform='instagram', opt_in_only=False)
                contacts_out = [{'id': c['id'], 'psid': c['platform_user_id'], 'name': c['display_name'] or 'Instagram User', 'last_message': None} for c in db_contacts]
                msg = (
                    f"Live sync unavailable: the app needs Meta App Review approval for "
                    f"instagram_manage_messages. Showing {len(contacts_out)} contact(s) from local database (contacts who have messaged via webhook)."
                )
                return jsonify({'success': True, 'synced': len(contacts_out), 'contacts': contacts_out, 'warning': msg})
            return jsonify({'success': False, 'error': err}), 400
        
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
        
        return jsonify({'success': True, 'synced': synced_count, 'contacts': new_contacts})
        
    except Exception as e:
        logger.error(f"Instagram sync error: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/contacts/<int:contact_id>/messages', methods=['GET'])
@login_required
def get_contact_messages(contact_id):
    """Return DB-stored message history for any contact (outgoing + webhook-saved inbound)."""
    with get_db_cursor(commit=False) as cursor:
        cursor.execute('''
            SELECT m.id, m.direction, m.message AS content, m.sent_at AS timestamp, c.display_name
            FROM messages m
            JOIN contacts c ON m.contact_id = c.id
            WHERE m.contact_id = %s
            ORDER BY m.sent_at ASC
            LIMIT 200
        ''', (contact_id,))
        messages = [dict(row) for row in cursor.fetchall()]
    return jsonify({'success': True, 'messages': messages})

@app.route('/api/instagram/conversations/<psid>', methods=['GET'])
@login_required
def get_instagram_conversation(psid):
    instagram_adapter = adapters['instagram']
    if not instagram_adapter.is_configured:
        return jsonify({'success': False, 'error': 'Instagram not configured'}), 400
    
    messages = instagram_adapter.get_conversation_history(psid, limit=100)
    
    return jsonify({'success': True, 'messages': messages, 'count': len(messages)})

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
        
        try:
            for i, recipient in enumerate(recipients):
                try:
                    result = adapter.send_message(recipient['platform_user_id'], message)
                except Exception as e:
                    logger.error(f"Broadcast send exception for {recipient['platform_user_id']}: {e}")
                    result = {'success': False, 'error': str(e)}
                
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
                    logger.error(f"Broadcast {broadcast_id} failed for {recipient['platform_user_id']}: {result.get('error')}")
                
                update_broadcast_stats(broadcast_id, sent_count, failed_count)
                socketio.emit('broadcast_progress', {
                    'broadcast_id': broadcast_id,
                    'index': i + 1,
                    'total': len(recipients),
                    'sent': sent_count,
                    'failed': failed_count,
                    'name': recipient.get('display_name') or recipient.get('platform_user_id', ''),
                    'success': result.get('success', False),
                    'error': result.get('error')
                })
                
                if i < len(recipients) - 1:
                    time.sleep(rate_limit)
        except Exception as e:
            logger.error(f"Broadcast {broadcast_id} thread crashed: {e}")
        finally:
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

@app.route('/api/contacts', methods=['POST'])
@login_required
def add_contact():
    data = request.json
    platform = data.get('platform')
    platform_user_id = data.get('platform_user_id', '').strip()
    display_name = data.get('display_name', '').strip()
    phone_number = data.get('phone_number', '').strip() or None
    opt_in = data.get('opt_in', True)

    if not platform or not platform_user_id:
        return jsonify({'success': False, 'error': 'Platform and Platform User ID are required'}), 400

    contact_id = save_contact(platform, platform_user_id, display_name or None, phone_number, opt_in)
    return jsonify({'success': True, 'contact_id': contact_id, 'message': 'Contact added successfully'})


@app.route('/api/contacts/import-csv', methods=['POST'])
@login_required
def import_contacts_csv():
    if 'file' not in request.files:
        return jsonify({'success': False, 'error': 'No file uploaded'}), 400

    file = request.files['file']
    if not file.filename.endswith('.csv'):
        return jsonify({'success': False, 'error': 'File must be a .csv'}), 400

    stream = io.StringIO(file.stream.read().decode('utf-8-sig'), newline=None)
    reader = csv.DictReader(stream)

    required_fields = {'platform', 'platform_user_id'}
    if not required_fields.issubset({f.strip().lower() for f in (reader.fieldnames or [])}):
        return jsonify({'success': False, 'error': 'CSV must have columns: platform, platform_user_id (optional: display_name, phone_number, opt_in)'}), 400

    added = 0
    errors = []
    for i, row in enumerate(reader, start=2):
        row = {k.strip().lower(): v.strip() for k, v in row.items()}
        platform = row.get('platform', '').lower()
        platform_user_id = row.get('platform_user_id', '')
        if not platform or not platform_user_id:
            errors.append(f'Row {i}: missing platform or platform_user_id')
            continue
        display_name = row.get('display_name') or None
        phone_number = row.get('phone_number') or None
        opt_in = row.get('opt_in', 'true').lower() not in ('false', '0', 'no')
        try:
            save_contact(platform, platform_user_id, display_name, phone_number, opt_in)
            added += 1
        except Exception as e:
            errors.append(f'Row {i}: {str(e)}')

    return jsonify({'success': True, 'added': added, 'errors': errors})


@app.route('/api/contacts', methods=['GET'])
@login_required
def get_contacts():
    platform = request.args.get('platform')
    opt_in_only = request.args.get('opt_in_only', 'false').lower() == 'true'
    search = request.args.get('search', '')

    # Ensure old Facebook contacts are imported even if webhook was added later.
    if platform in (None, '', 'facebook'):
        fb_sync = maybe_backfill_facebook_contacts(force=False)
        if not fb_sync.get('success') and not fb_sync.get('skipped'):
            logger.warning(f"Facebook backfill skipped due to error: {fb_sync.get('error')}")
    
    contacts = get_all_contacts(platform, opt_in_only, search)
    
    return jsonify({'success': True, 'contacts': contacts, 'count': len(contacts)})

@app.route('/api/contacts/<int:contact_id>', methods=['PUT'])
@login_required
def update_contact(contact_id):
    data = request.json
    
    with get_db_cursor(commit=True) as cursor:
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
    with get_db_cursor(commit=True) as cursor:
        cursor.execute("DELETE FROM contacts WHERE id = %s", (contact_id,))
    
    return jsonify({'success': True, 'message': 'Contact deleted'})

@app.route('/api/contacts/bulk-opt-in', methods=['POST'])
@login_required
def bulk_opt_in():
    data = request.json
    contact_ids = data.get('contact_ids', [])
    
    if not contact_ids:
        return jsonify({'success': False, 'error': 'No contacts selected'}), 400
    
    with get_db_cursor(commit=True) as cursor:
        placeholders = ','.join(['%s'] * len(contact_ids))
        cursor.execute(f'''
            UPDATE contacts 
            SET opt_in = TRUE, opt_in_date = %s, updated_at = %s
            WHERE id IN ({placeholders})
        ''', [datetime.now(), datetime.now()] + contact_ids)
    
    return jsonify({'success': True, 'updated': len(contact_ids)})

# ==================== BROADCAST ROUTES ====================

@app.route('/api/broadcasts', methods=['GET'])
@login_required
def get_broadcasts_api():
    broadcasts = get_broadcasts()
    return jsonify({'success': True, 'broadcasts': broadcasts})

@app.route('/api/broadcasts/<int:broadcast_id>', methods=['GET'])
@login_required
def get_broadcast_details(broadcast_id):
    with get_db_cursor(commit=False) as cursor:
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
    
    return jsonify({'success': True, 'broadcast': dict(broadcast), 'recipients': [dict(r) for r in recipients]})


@app.route('/api/broadcasts/<int:broadcast_id>/cancel', methods=['POST'])
@login_required
def cancel_broadcast(broadcast_id):
    """Unstick a broadcast stuck in processing"""
    with get_db_cursor(commit=True) as cursor:
        cursor.execute(
            "UPDATE broadcasts SET status = 'cancelled', completed_at = %s WHERE id = %s AND status = 'processing'",
            (datetime.now(), broadcast_id)
        )
    return jsonify({'success': True, 'message': 'Broadcast cancelled'})


@app.route('/api/broadcasts/<int:broadcast_id>/rerun', methods=['POST'])
@login_required
def rerun_broadcast(broadcast_id):
    """Rerun a previous broadcast with the same settings"""
    with get_db_cursor(commit=False) as cursor:
        cursor.execute("SELECT * FROM broadcasts WHERE id = %s", (broadcast_id,))
        original = cursor.fetchone()

    if not original:
        return jsonify({'success': False, 'error': 'Broadcast not found'}), 404

    platform = original['platform']
    message = original['message']
    audience_filter = original['audience_filter'] or 'all'
    campaign_name = f"{original['name']} (Rerun)"
    rate_limit = 1

    if platform not in adapters:
        return jsonify({'success': False, 'error': f'Invalid platform: {platform}'}), 400

    adapter = adapters[platform]
    if not adapter.is_configured:
        return jsonify({'success': False, 'error': f'{platform} is not configured'}), 400

    recipients = get_recipients_for_broadcast(platform, audience_filter)
    if not recipients:
        return jsonify({'success': False, 'error': 'No recipients found for this platform/audience'}), 404

    new_broadcast_id = create_broadcast_record(
        session['user_id'], campaign_name, platform, message,
        audience_filter, len(recipients)
    )

    def process_broadcast():
        sent_count = 0
        failed_count = 0
        try:
            for i, recipient in enumerate(recipients):
                try:
                    result = adapter.send_message(recipient['platform_user_id'], message)
                except Exception as e:
                    result = {'success': False, 'error': str(e)}
                add_broadcast_recipient(
                    new_broadcast_id, recipient['id'],
                    'sent' if result.get('success') else 'failed',
                    result.get('error')
                )
                if result.get('success'):
                    sent_count += 1
                    save_message(recipient['id'], platform, 'outgoing', message)
                else:
                    failed_count += 1
                update_broadcast_stats(new_broadcast_id, sent_count, failed_count)
                socketio.emit('broadcast_progress', {
                    'broadcast_id': new_broadcast_id,
                    'index': i + 1,
                    'total': len(recipients),
                    'sent': sent_count,
                    'failed': failed_count,
                    'name': recipient.get('display_name') or recipient.get('platform_user_id', ''),
                    'success': result.get('success', False),
                    'error': result.get('error')
                })
                if i < len(recipients) - 1:
                    time.sleep(rate_limit)
        except Exception as e:
            logger.error(f"Rerun broadcast {new_broadcast_id} thread crashed: {e}")
        finally:
            complete_broadcast(new_broadcast_id)
            socketio.emit('broadcast_completed', {
                'broadcast_id': new_broadcast_id,
                'sent': sent_count,
                'failed': failed_count,
                'total': len(recipients)
            })

    thread = threading.Thread(target=process_broadcast)
    thread.daemon = True
    thread.start()

    return jsonify({
        'success': True,
        'broadcast_id': new_broadcast_id,
        'total_recipients': len(recipients),
        'message': f'Rerun started. Sending to {len(recipients)} recipients.'
    })


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

@app.route('/api/messages', methods=['GET'])
@login_required
def get_messages_api():
    limit = request.args.get('limit', 50, type=int)
    platform = request.args.get('platform')
    messages = get_messages(limit, platform)
    return jsonify({'success': True, 'messages': messages})

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

@app.route('/api/db-test', methods=['GET'])
@login_required
def db_test():
    try:
        with get_db_cursor(commit=False) as cursor:
            cursor.execute("SELECT COUNT(*) as contacts FROM contacts")
            contacts = cursor.fetchone()
            cursor.execute("SELECT COUNT(*) as broadcasts FROM broadcasts")
            broadcasts = cursor.fetchone()
            cursor.execute("SELECT COUNT(*) as messages FROM messages")
            messages = cursor.fetchone()
        return jsonify({
            'success': True,
            'db_connected': True,
            'contacts': contacts['contacts'],
            'broadcasts': broadcasts['broadcasts'],
            'messages': messages['messages']
        })
    except Exception as e:
        return jsonify({'success': False, 'db_connected': False, 'error': str(e)}), 500

# ==================== WEBHOOK ROUTES ====================

@app.route('/webhook/instagram', methods=['GET', 'POST'])
def instagram_webhook():
    if request.method == 'GET':
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
        return jsonify({'status': 'ok'}), 200
    except Exception as e:
        logger.error(f"Webhook error: {e}")
        return jsonify({'status': 'error'}), 500

# ==================== SOCKET.IO EVENTS ====================

@socketio.on('connect')
def handle_connect():
    emit('connected', {'message': 'Connected to server'})

# ==================== RUN THE APP ====================

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    debug = os.environ.get('FLASK_DEBUG', 'False').lower() == 'true'
    
    print("=" * 60)
    print("UNIFIED SOCIAL MEDIA MESSAGING SYSTEM - FULLY FIXED")
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