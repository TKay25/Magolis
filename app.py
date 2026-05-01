"""
Unified Social Media Messaging System with Bulk Broadcast
Complete Flask Backend - Production Ready with Facebook Contact Sync
"""

import os
import json
import time
import sqlite3
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

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Flask app
app = Flask(__name__, template_folder='templates', static_folder='static')
app.secret_key = os.getenv('SECRET_KEY', 'your-super-secret-key-change-this')
app.permanent_session_lifetime = timedelta(days=7)

# CORS: Allow credentials and restrict origins for security
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

# Favicon route to fix 404
@app.route('/favicon.ico')
def favicon():
    return send_from_directory(os.path.join(app.root_path, 'static'), 'favicon.ico', mimetype='image/vnd.microsoft.icon')

# Database setup
DATABASE = 'unified_messaging.db'

def get_db():
    """Get database connection"""
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = sqlite3.connect(DATABASE)
        db.row_factory = sqlite3.Row
    return db

@app.teardown_appcontext
def close_connection(exception):
    """Close database connection"""
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()

def init_db():
    """Initialize database tables"""
    with app.app_context():
        db = get_db()
        cursor = db.cursor()
        
        # Users table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
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
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                platform TEXT NOT NULL,
                platform_user_id TEXT NOT NULL,
                display_name TEXT,
                phone_number TEXT,
                email TEXT,
                opt_in BOOLEAN DEFAULT 0,
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
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                contact_id INTEGER,
                platform TEXT NOT NULL,
                direction TEXT NOT NULL,
                message TEXT NOT NULL,
                status TEXT DEFAULT 'pending',
                message_id TEXT,
                sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                delivered_at TIMESTAMP,
                read_at TIMESTAMP,
                FOREIGN KEY (contact_id) REFERENCES contacts(id)
            )
        ''')
        
        # Broadcast campaigns table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS broadcasts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
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
        
        # Broadcast recipients tracking
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS broadcast_recipients (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                broadcast_id INTEGER,
                contact_id INTEGER,
                status TEXT DEFAULT 'pending',
                error_message TEXT,
                sent_at TIMESTAMP,
                FOREIGN KEY (broadcast_id) REFERENCES broadcasts(id),
                FOREIGN KEY (contact_id) REFERENCES contacts(id)
            )
        ''')
        
        # Insert default admin user
        cursor.execute("SELECT id FROM users WHERE username = 'admin'")
        if not cursor.fetchone():
            cursor.execute('''
                INSERT INTO users (username, password, email, full_name, role)
                VALUES (?, ?, ?, ?, ?)
            ''', ('admin', generate_password_hash('admin123'), 'admin@example.com', 'Administrator', 'admin'))
        
        db.commit()
        logger.info("Database initialized successfully")

# Initialize database
init_db()

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
        self.is_configured = bool(self.page_access_token)
    
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
        """Get the Page ID associated with the access token"""
        try:
            url = "https://graph.facebook.com/v18.0/me/accounts"
            response = requests.get(url, params={'access_token': self.page_access_token})
            data = response.json()
            if data.get('data') and len(data['data']) > 0:
                return data['data'][0]['id']
            return None
        except:
            return None
    
    def get_conversations(self, limit=50):
        """Fetch conversations from Facebook Page"""
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


class InstagramAdapter:
    def __init__(self):
        self.access_token = os.getenv('INSTAGRAM_ACCESS_TOKEN')
        self.business_id = os.getenv('INSTAGRAM_BUSINESS_ID')
        self.is_configured = bool(self.access_token and self.business_id)
    
    def send_message(self, recipient_id, content):
        if not self.is_configured:
            return {'success': False, 'error': 'Instagram not configured'}
        
        url = f"https://graph.facebook.com/v18.0/{self.business_id}/messages"
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
            return {'success': False, 'error': 'Failed to send'}
        except Exception as e:
            return {'success': False, 'error': str(e)}


class LinkedInAdapter:
    def __init__(self):
        self.access_token = os.getenv('LINKEDIN_ACCESS_TOKEN')
        self.is_configured = bool(self.access_token)
    
    def send_message(self, recipient_id, content):
        if not self.is_configured:
            return {'success': False, 'error': 'LinkedIn not configured'}
        # LinkedIn API placeholder
        return {'success': False, 'error': 'LinkedIn API coming soon'}


# Initialize adapters
adapters = {
    'whatsapp': WhatsAppAdapter(),
    'facebook': FacebookAdapter(),
    'twitter': TwitterAdapter(),
    'instagram': InstagramAdapter(),
    'linkedin': LinkedInAdapter()
}

# ==================== HELPER FUNCTIONS ====================

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            return jsonify({'success': False, 'error': 'Authentication required'}), 401
        return f(*args, **kwargs)
    return decorated_function

def save_contact(platform, platform_user_id, display_name=None, phone_number=None, opt_in=True):
    db = get_db()
    cursor = db.cursor()
    
    cursor.execute(
        "SELECT id FROM contacts WHERE platform = ? AND platform_user_id = ?",
        (platform, str(platform_user_id))
    )
    existing = cursor.fetchone()
    
    now = datetime.now().isoformat()
    
    if existing:
        cursor.execute('''
            UPDATE contacts 
            SET display_name = COALESCE(?, display_name),
                phone_number = COALESCE(?, phone_number),
                last_interaction = ?,
                updated_at = ?
            WHERE id = ?
        ''', (display_name, phone_number, now, now, existing['id']))
        contact_id = existing['id']
    else:
        cursor.execute('''
            INSERT INTO contacts (platform, platform_user_id, display_name, phone_number, opt_in, opt_in_date, last_interaction)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (platform, str(platform_user_id), display_name, phone_number, opt_in, now if opt_in else None, now))
        contact_id = cursor.lastrowid
    
    db.commit()
    return contact_id

def save_message(contact_id, platform, direction, message, status='sent'):
    db = get_db()
    cursor = db.cursor()
    cursor.execute('''
        INSERT INTO messages (contact_id, platform, direction, message, status)
        VALUES (?, ?, ?, ?, ?)
    ''', (contact_id, platform, direction, message, status))
    db.commit()

def get_recipients_for_broadcast(platform, audience_filter='all', tags=None):
    db = get_db()
    cursor = db.cursor()
    
    query = "SELECT * FROM contacts WHERE platform = ? AND opt_in = 1"
    params = [platform]
    
    if audience_filter == 'active':
        query += " AND last_interaction > datetime('now', '-30 days')"
    elif audience_filter == 'tagged' and tags:
        like_clauses = []
        for tag in tags.split(','):
            like_clauses.append("tags LIKE ?")
            params.append(f"%{tag.strip()}%")
        query += " AND (" + " OR ".join(like_clauses) + ")"
    
    cursor.execute(query, params)
    return cursor.fetchall()

# ==================== FACEBOOK CONTACT SYNC ROUTES ====================

@app.route('/api/facebook/sync-contacts', methods=['POST'])
@login_required
def sync_facebook_contacts():
    """Fetch all conversations from Facebook Page and sync to contacts database"""
    
    facebook_adapter = adapters['facebook']
    if not facebook_adapter.is_configured:
        return jsonify({'success': False, 'error': 'Facebook not configured'}), 400
    
    result = facebook_adapter.get_conversations(limit=100)
    
    if not result['success']:
        return jsonify({'success': False, 'error': result['error']}), 400
    
    synced_count = 0
    new_contacts = []
    
    for conv in result['conversations']:
        psid = conv['psid']
        user_name = conv['name']
        last_interaction = conv.get('last_interaction')
        
        # Save to database
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


@app.route('/api/facebook/conversations/<psid>', methods=['GET'])
@login_required
def get_facebook_conversation(psid):
    """Get full message history with a specific Facebook user"""
    
    facebook_adapter = adapters['facebook']
    if not facebook_adapter.is_configured:
        return jsonify({'success': False, 'error': 'Facebook not configured'}), 400
    
    page_id = facebook_adapter.get_page_id()
    if not page_id:
        return jsonify({'success': False, 'error': 'Could not get Page ID'}), 400
    
    try:
        # Get conversation ID for this user
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
        
        # Get messages
        messages_url = f"https://graph.facebook.com/v18.0/{conversation_id}/messages"
        msg_params = {
            'access_token': facebook_adapter.page_access_token,
            'fields': 'message,created_time,from,id',
            'limit': 100
        }
        
        msg_response = requests.get(messages_url, params=msg_params)
        messages_data = msg_response.json()
        
        # Format messages
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

# ==================== ROUTES ====================

# Login page route
@app.route('/login')
def login_page():
    return render_template('login.html')

# Main dashboard page, redirect to login if not authenticated
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
    
    db = get_db()
    cursor = db.cursor()
    cursor.execute("SELECT id, username, password, full_name, role FROM users WHERE username = ?", (username,))
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
    
    # Handle Twitter username to ID conversion
    if platform == 'twitter' and not recipient.isdigit():
        user_id = adapters['twitter'].get_user_id(recipient)
        if not user_id:
            return jsonify({'success': False, 'error': 'Could not find Twitter user'}), 400
        recipient = user_id
    
    result = adapters[platform].send_message(recipient, content)
    
    if result.get('success'):
        contact_id = save_contact(platform, recipient, display_name, recipient if platform == 'whatsapp' else None, True)
        save_message(contact_id, platform, 'outgoing', content)
        
        # Notify via WebSocket
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
    
    # Get recipients
    recipients = get_recipients_for_broadcast(platform, audience_filter, tags)
    
    if not recipients:
        return jsonify({'success': False, 'error': 'No recipients found matching criteria'}), 404
    
    # Create broadcast record
    db = get_db()
    cursor = db.cursor()
    cursor.execute('''
        INSERT INTO broadcasts (user_id, name, platform, message, audience_filter, total_recipients, status)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', (session['user_id'], campaign_name, platform, message, audience_filter, len(recipients), 'processing'))
    broadcast_id = cursor.lastrowid
    db.commit()
    
    # Process broadcast in background
    def process_broadcast():
        sent_count = 0
        failed_count = 0
        
        for i, recipient in enumerate(recipients):
            result = adapter.send_message(recipient['platform_user_id'], message)
            
            cursor.execute('''
                INSERT INTO broadcast_recipients (broadcast_id, contact_id, status, error_message, sent_at)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            ''', (broadcast_id, recipient['id'], 'sent' if result.get('success') else 'failed', result.get('error')))
            
            if result.get('success'):
                sent_count += 1
                save_message(recipient['id'], platform, 'outgoing', message)
            else:
                failed_count += 1
            
            cursor.execute('''
                UPDATE broadcasts SET sent_count = ?, failed_count = ? WHERE id = ?
            ''', (sent_count, failed_count, broadcast_id))
            db.commit()
            
            if i < len(recipients) - 1:
                time.sleep(rate_limit)
        
        cursor.execute('''
            UPDATE broadcasts SET status = 'completed', completed_at = CURRENT_TIMESTAMP WHERE id = ?
        ''', (broadcast_id,))
        db.commit()
        
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

@app.route('/api/contacts', methods=['GET'])
@login_required
def get_contacts():
    platform = request.args.get('platform')
    opt_in_only = request.args.get('opt_in_only', 'false').lower() == 'true'
    search = request.args.get('search', '')
    
    db = get_db()
    cursor = db.cursor()
    
    query = "SELECT * FROM contacts WHERE 1=1"
    params = []
    
    if platform:
        query += " AND platform = ?"
        params.append(platform)
    
    if opt_in_only:
        query += " AND opt_in = 1"
    
    if search:
        query += " AND (display_name LIKE ? OR platform_user_id LIKE ? OR phone_number LIKE ?)"
        search_param = f"%{search}%"
        params.extend([search_param, search_param, search_param])
    
    query += " ORDER BY last_interaction DESC NULLS LAST LIMIT 100"
    
    cursor.execute(query, params)
    contacts = cursor.fetchall()
    
    return jsonify({
        'success': True,
        'contacts': [dict(contact) for contact in contacts],
        'count': len(contacts)
    })

@app.route('/api/contacts/<int:contact_id>', methods=['PUT'])
@login_required
def update_contact(contact_id):
    data = request.json
    
    db = get_db()
    cursor = db.cursor()
    cursor.execute('''
        UPDATE contacts 
        SET display_name = COALESCE(?, display_name),
            phone_number = COALESCE(?, phone_number),
            email = COALESCE(?, email),
            tags = COALESCE(?, tags),
            notes = COALESCE(?, notes),
            opt_in = COALESCE(?, opt_in),
            updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
    ''', (
        data.get('display_name'),
        data.get('phone_number'),
        data.get('email'),
        data.get('tags'),
        data.get('notes'),
        data.get('opt_in'),
        contact_id
    ))
    db.commit()
    
    return jsonify({'success': True, 'message': 'Contact updated'})

@app.route('/api/contacts/<int:contact_id>', methods=['DELETE'])
@login_required
def delete_contact(contact_id):
    db = get_db()
    cursor = db.cursor()
    cursor.execute("DELETE FROM contacts WHERE id = ?", (contact_id,))
    db.commit()
    return jsonify({'success': True, 'message': 'Contact deleted'})

@app.route('/api/contacts/bulk-opt-in', methods=['POST'])
@login_required
def bulk_opt_in():
    data = request.json
    contact_ids = data.get('contact_ids', [])
    
    if not contact_ids:
        return jsonify({'success': False, 'error': 'No contacts selected'}), 400
    
    db = get_db()
    cursor = db.cursor()
    placeholders = ','.join('?' * len(contact_ids))
    cursor.execute(f'''
        UPDATE contacts 
        SET opt_in = 1, opt_in_date = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
        WHERE id IN ({placeholders})
    ''', contact_ids)
    db.commit()
    
    return jsonify({'success': True, 'updated': cursor.rowcount})

@app.route('/api/messages', methods=['GET'])
@login_required
def get_messages():
    limit = request.args.get('limit', 50, type=int)
    platform = request.args.get('platform')
    
    db = get_db()
    cursor = db.cursor()
    
    query = '''
        SELECT m.*, c.display_name, c.platform_user_id
        FROM messages m
        JOIN contacts c ON m.contact_id = c.id
        WHERE 1=1
    '''
    params = []
    
    if platform:
        query += " AND m.platform = ?"
        params.append(platform)
    
    query += " ORDER BY m.sent_at DESC LIMIT ?"
    params.append(limit)
    
    cursor.execute(query, params)
    messages = cursor.fetchall()
    
    return jsonify({
        'success': True,
        'messages': [dict(m) for m in messages],
        'count': len(messages)
    })

@app.route('/api/broadcasts', methods=['GET'])
@login_required
def get_broadcasts():
    db = get_db()
    cursor = db.cursor()
    cursor.execute('''
        SELECT * FROM broadcasts 
        ORDER BY created_at DESC 
        LIMIT 20
    ''')
    broadcasts = cursor.fetchall()
    
    return jsonify({
        'success': True,
        'broadcasts': [dict(b) for b in broadcasts]
    })

@app.route('/api/broadcasts/<int:broadcast_id>', methods=['GET'])
@login_required
def get_broadcast_details(broadcast_id):
    db = get_db()
    cursor = db.cursor()
    
    cursor.execute("SELECT * FROM broadcasts WHERE id = ?", (broadcast_id,))
    broadcast = cursor.fetchone()
    
    if not broadcast:
        return jsonify({'success': False, 'error': 'Broadcast not found'}), 404
    
    cursor.execute('''
        SELECT br.*, c.display_name, c.platform_user_id
        FROM broadcast_recipients br
        JOIN contacts c ON br.contact_id = c.id
        WHERE br.broadcast_id = ?
    ''', (broadcast_id,))
    recipients = cursor.fetchall()
    
    return jsonify({
        'success': True,
        'broadcast': dict(broadcast),
        'recipients': [dict(r) for r in recipients]
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
def get_dashboard_stats():
    db = get_db()
    cursor = db.cursor()
    
    cursor.execute('SELECT COUNT(*) as total, SUM(CASE WHEN opt_in = 1 THEN 1 ELSE 0 END) as opted_in FROM contacts')
    contact_stats = cursor.fetchone()
    
    cursor.execute('SELECT platform, COUNT(*) as count FROM contacts GROUP BY platform')
    contacts_by_platform = cursor.fetchall()
    
    cursor.execute('SELECT COUNT(*) as total, SUM(CASE WHEN direction = "outgoing" THEN 1 ELSE 0 END) as sent FROM messages')
    message_stats = cursor.fetchone()
    
    cursor.execute('SELECT COUNT(*) as total FROM broadcasts')
    broadcast_stats = cursor.fetchone()
    
    cursor.execute('''
        SELECT COUNT(*) as active FROM contacts 
        WHERE last_interaction > datetime("now", "-30 days")
    ''')
    active_stats = cursor.fetchone()
    
    return jsonify({
        'success': True,
        'stats': {
            'total_contacts': contact_stats['total'] if contact_stats else 0,
            'opted_in_contacts': contact_stats['opted_in'] if contact_stats else 0,
            'active_contacts_30d': active_stats['active'] if active_stats else 0,
            'sent_messages': message_stats['sent'] if message_stats else 0,
            'total_broadcasts': broadcast_stats['total'] if broadcast_stats else 0,
            'contacts_by_platform': [dict(c) for c in contacts_by_platform]
        }
    })

@app.route('/api/recipient-count', methods=['POST'])
@login_required
def get_recipient_count():
    data = request.json
    platform = data.get('platform')
    audience_filter = data.get('audience_filter', 'all')
    tags = data.get('tags')
    
    recipients = get_recipients_for_broadcast(platform, audience_filter, tags)
    return jsonify({'success': True, 'count': len(recipients)})

@app.route('/health', methods=['GET'])
def health_check():
    configured_count = sum(1 for a in adapters.values() if a.is_configured)
    return jsonify({
        'status': 'healthy',
        'configured_platforms': configured_count,
        'total_platforms': len(adapters),
        'timestamp': datetime.now().isoformat()
    })

# Socket.IO events
@socketio.on('connect')
def handle_connect():
    emit('connected', {'message': 'Connected to server'})

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    debug = os.environ.get('FLASK_DEBUG', 'False').lower() == 'true'
    
    print("=" * 60)
    print("UNIFIED SOCIAL MEDIA MESSAGING SYSTEM")
    print("=" * 60)
    print(f"\nServer running on port: {port}")
    print("\nConfigured Platforms:")
    for name, adapter in adapters.items():
        status = "✓ CONFIGURED" if adapter.is_configured else "✗ NOT CONFIGURED"
        print(f"  • {name.upper()}: {status}")
    
    print("\nDefault Admin Login:")
    print("  Username: admin")
    print("  Password: admin123")
    print("\nFacebook Sync Available:")
    print("  • POST /api/facebook/sync-contacts - Sync Facebook contacts")
    print("  • GET /api/facebook/conversations/<psid> - Get conversation history")
    print("\n" + "=" * 60)
    
    socketio.run(app, host='0.0.0.0', port=port, debug=debug)