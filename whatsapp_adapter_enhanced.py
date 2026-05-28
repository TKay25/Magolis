"""
Enhanced WhatsApp Adapter with Interactive Message Support
Add these methods to your existing WhatsAppAdapter class
"""

import requests
import logging
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


class EnhancedWhatsAppAdapter:
    """Add these methods to your existing WhatsAppAdapter class"""
    
    def send_interactive_message(self, recipient_id: str, interactive_content: Dict) -> Dict:
        """
        Send interactive message (buttons or list)
        
        Args:
            recipient_id: WhatsApp user ID
            interactive_content: Dict with interactive structure
        """
        if not self.is_configured:
            return {'success': False, 'error': self.init_error or 'WhatsApp not configured'}
        
        to = recipient_id.lstrip('+')
        url = f"https://graph.facebook.com/v18.0/{self.phone_number_id}/messages"
        
        # Build payload from interactive content
        payload = {
            "messaging_product": "whatsapp",
            "recipient_type": "individual",
            "to": to,
            **interactive_content  # Contains type and either text or interactive
        }
        
        try:
            response = requests.post(url, json=payload, 
                                    headers=self._headers(), timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                if data.get('messages'):
                    return {
                        'success': True, 
                        'platform': 'whatsapp', 
                        'message_id': data['messages'][0].get('id')
                    }
            
            error_data = response.json()
            error_msg = error_data.get('error', {}).get('message', f'HTTP {response.status_code}')
            logger.error(f"WhatsApp interactive send failed: {error_msg}")
            return {'success': False, 'error': error_msg}
            
        except Exception as e:
            logger.error(f"WhatsApp interactive send exception: {e}")
            return {'success': False, 'error': str(e)}
    
    def send_template_message(self, recipient_id: str, template_name: str, 
                               language_code: str = 'en_US', 
                               components: List[Dict] = None) -> Dict:
        """
        Send a template message (for out-of-24h-window communication)
        
        Args:
            recipient_id: WhatsApp user ID
            template_name: Name of approved template
            language_code: Language code (default 'en_US')
            components: Optional template components
        """
        if not self.is_configured:
            return {'success': False, 'error': 'WhatsApp not configured'}
        
        to = recipient_id.lstrip('+')
        url = f"https://graph.facebook.com/v18.0/{self.phone_number_id}/messages"
        
        payload = {
            "messaging_product": "whatsapp",
            "to": to,
            "type": "template",
            "template": {
                "name": template_name,
                "language": {"code": language_code}
            }
        }
        
        if components:
            payload["template"]["components"] = components
        
        try:
            response = requests.post(url, json=payload, 
                                    headers=self._headers(), timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                return {
                    'success': True,
                    'platform': 'whatsapp',
                    'message_id': data.get('messages', [{}])[0].get('id')
                }
            
            error_data = response.json()
            return {'success': False, 'error': error_data.get('error', {}).get('message')}
            
        except Exception as e:
            logger.error(f"Template send error: {e}")
            return {'success': False, 'error': str(e)}
    
    def get_template_list(self) -> Dict:
        """Get list of approved templates"""
        if not self.is_configured:
            return {'success': False, 'error': 'WhatsApp not configured'}
        
        url = f"https://graph.facebook.com/v18.0/{self.phone_number_id}/message_templates"
        
        try:
            response = requests.get(url, headers=self._headers(), timeout=30)
            data = response.json()
            
            if 'data' in data:
                return {'success': True, 'templates': data['data']}
            
            return {'success': False, 'error': data.get('error', {}).get('message')}
            
        except Exception as e:
            return {'success': False, 'error': str(e)}