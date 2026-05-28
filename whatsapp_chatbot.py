"""
Stephen Margolis Resort WhatsApp Chatbot - Meta API Compliant
Respects: 3 buttons max OR 10 list rows max
"""

from email.mime import text
import os
import json
import re
import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class WhatsAppInteractiveMenu:
    """Build Meta-compliant WhatsApp interactive messages"""
    
    @staticmethod
    def create_list_message(text: str, sections: List[Dict]) -> Dict:
        """
        Create interactive list message (max 10 rows total across sections)
        
        Args:
            text: Main message body
            sections: List of section dicts with 'title' and 'rows'
                     Each row: {'id': 'unique_id', 'title': 'Display text (max 24 chars)', 
                               'description': 'Optional (max 72 chars)'}
        Returns:
            WhatsApp API compatible dict
        """
        # Validate and truncate total rows to 10
        total_rows = 0
        truncated_sections = []
        
        for section in sections:
            rows = section.get('rows', [])
            remaining = 10 - total_rows
            if remaining <= 0:
                break
            if len(rows) > remaining:
                rows = rows[:remaining]
            truncated_sections.append({
                'title': section.get('title', '')[:60],
                'rows': rows
            })
            total_rows += len(rows)
        
        return {
            "messaging_product": "whatsapp",
            "recipient_type": "individual",
            "type": "interactive",
            "interactive": {
                "type": "list",
                "header": {
                    "type": "text",
                    "text": "🏨 Stephen Margolis Resort"
                },
                "body": {
                    "text": text[:1024]
                },
                "footer": {
                    "text": "Tap an option below"
                },
                "action": {
                    "button": "📋 View Menu",
                    "sections": truncated_sections
                }
            }
        }
    
    @staticmethod
    def create_button_message(text: str, buttons: List[Dict]) -> Dict:
        """
        Create interactive button message (max 3 buttons)
        
        Args:
            text: Main message body
            buttons: List of button dicts with 'id' and 'title' (max 20 chars)
        Returns:
            WhatsApp API compatible dict
        """
        # Validate button count
        if len(buttons) > 3:
            buttons = buttons[:3]
        
        # Validate button titles
        for btn in buttons:
            if len(btn.get('title', '')) > 20:
                btn['title'] = btn['title'][:17] + "..."
        
        return {
            "messaging_product": "whatsapp",
            "recipient_type": "individual",
            "type": "interactive",
            "interactive": {
                "type": "button",
                "body": {
                    "text": text[:1024]
                },
                "action": {
                    "buttons": [
                        {
                            "type": "reply",
                            "reply": {
                                "id": btn['id'],
                                "title": btn['title'][:20]
                            }
                        } for btn in buttons
                    ]
                }
            }
        }
    
    @staticmethod
    def create_text_message(text: str) -> Dict:
        """Create simple text message"""
        return {
            "messaging_product": "whatsapp",
            "recipient_type": "individual",
            "type": "text",
            "text": {
                "body": text[:4096]
            }
        }


class StephenMargolisChatbot:
    """Meta API compliant chatbot with tiered menus"""
    
    def __init__(self):
        self.user_sessions = {}
        
        # Main menu - uses LIST message (10 options max)
        self.main_menu = {
            'text': "🏨 *WELCOME TO STEPHEN MARGOLIS RESORT* 🏨\n\nPlease select an option below to learn more about our services:",
            'sections': [
                {
                    'title': "🎯 ACTIVITIES",
                    'rows': [
                        {'id': 'activities', 'title': 'Activities & Entrance', 
                         'description': 'Fees, activities, braai stands'},
                        {'id': 'accommodation', 'title': 'Accommodation', 
                         'description': 'Rooms, rates, booking info'},
                        {'id': 'restaurant', 'title': 'Restaurant & Menu', 
                         'description': 'Food, drinks, prices'}
                    ]
                },
                {
                    'title': "🎉 EVENTS",
                    'rows': [
                        {'id': 'birthday', 'title': 'Birthday Package', 
                         'description': 'Celebrate your special day'},
                        {'id': 'wedding', 'title': 'Weddings', 
                         'description': 'Venue, catering, packages'},
                        {'id': 'conference', 'title': 'Conferences', 
                         'description': 'Team building, meetings'},
                        {'id': 'educational', 'title': 'Educational Tours', 
                         'description': 'School trips, group rates'}
                    ]
                },
                {
                    'title': "📍 INFO & CONTACT",
                    'rows': [
                        {'id': 'getaway', 'title': 'Getaway Packages', 
                         'description': 'Multi-night stay deals'},
                        {'id': 'location', 'title': 'Location & Directions', 
                         'description': 'How to get to us'},
                        {'id': 'contact', 'title': 'Contact & Hours', 
                         'description': 'Phone, WhatsApp, email'}
                    ]
                }
            ]
        }
    
    def get_menu(self, menu_id: str) -> Tuple[Dict, str]:
        """Get interactive message for a menu, returns (message_dict, next_state)"""
        
        menus = {
            'activities': self._get_activities_menu(),
            'accommodation': self._get_accommodation_menu(),
            'restaurant': self._get_restaurant_menu(),
            'birthday': self._get_birthday_menu(),
            'wedding': self._get_wedding_menu(),
            'conference': self._get_conference_menu(),
            'educational': self._get_educational_menu(),
            'getaway': self._get_getaway_menu(),
            'location': self._get_location_menu(),
            'contact': self._get_contact_menu(),
        }
        
        if menu_id == 'main':
            return WhatsAppInteractiveMenu.create_list_message(
                self.main_menu['text'], 
                self.main_menu['sections']
            ), 'main'
        
        if menu_id in menus:
            return menus[menu_id], menu_id
        
        return WhatsAppInteractiveMenu.create_list_message(
            self.main_menu['text'], 
            self.main_menu['sections']
        ), 'main'
    
    def _get_activities_menu(self) -> Tuple[Dict, str]:
        text = """🎯 *ACTIVITIES & ENTRANCE FEES*

        *ENTRANCE:*
        • Adults: $5
        • Children (3-12yrs): $3

        *ACTIVITIES ($5 each):*
        Zipline • VR • Horse Riding
        Boat Cruise • Giant Swing
        Kids Play Area

        *OTHER:*
        Fishing: $10 | Canoeing: $3
        Putt-Putt Golf: FREE!

        *FREE:* Braai stands (bring own food)
        *PARKING:* $5/vehicle (after 1st 50 free)

        What would you like to do?"""
            
        buttons = [
            {'id': 'book_activity', 'title': '📅 Book Activity'},
            {'id': 'ask_activities', 'title': '❓ More Info'},
            {'id': 'main', 'title': '🏠 Main Menu'}
        ]
        
        return WhatsAppInteractiveMenu.create_button_message(text, buttons), 'activities'
    
    def _get_accommodation_menu(self) -> Tuple[Dict, str]:
        text = """🏠 *ACCOMMODATION* (per night, B&B)

*STANDARD ROOM:*
1 person: $50 | 2 people: $60

*DELUXE:* $70
*EXECUTIVE:* $80
*PLATINUM SUITE:* $100

*DAY REST:* $40 (4 hours)

📅 Check-in: 2pm | Check-out: 10am
🛏️ Extra bed: $35
💧 50% deposit required

Would you like to book or learn more?"""
        
        buttons = [
            {'id': 'book_room', 'title': '📅 Book Room'},
            {'id': 'room_amenities', 'title': '🏊 Amenities'},
            {'id': 'main', 'title': '🏠 Main Menu'}
        ]
        
        return WhatsAppInteractiveMenu.create_button_message(text, buttons), 'accommodation'
    
    def _get_restaurant_menu(self) -> Tuple[Dict, str]:
        text = """🍽️ *RESTAURANT MENU* (8am-9pm)

*STARTERS:* $5-6
Buffalo Wings • Beef/Chicken Kebabs
Veg Spring Rolls • Veg Samoosas

*MAIN COURSE:* $8-15
Beef/Veg Burger • 1/4 or 1/2 Chicken
T-bone Steak • Chicken Strips

*SIDES:* $3-4
Rice • Chips • Garden Salad

*DESSERT:* Ice Cream $4

Would you like to order?"""
        
        buttons = [
            {'id': 'order_food', 'title': '🍔 Place Order'},
            {'id': 'full_menu', 'title': '📋 Full Menu'},
            {'id': 'main', 'title': '🏠 Main Menu'}
        ]
        
        return WhatsAppInteractiveMenu.create_button_message(text, buttons), 'restaurant'
    
    def _get_birthday_menu(self) -> Tuple[Dict, str]:
        text = """🎂 *BIRTHDAY PACKAGE*

*Friends & Family Package:*
👥 Minimum 5 paying people
👤 BIRTHDAY PERSON: FREE!

*PRICES:*
Adults: $20 | Children: $18

*INCLUDES:*
✅ Entrance fee
✅ 1 activity per person
✅ Meal (1/4 chicken/burger)
✅ Complimentary tea/coffee
✅ Games Zone & Putt-Putt

To book, reply with your date and guest count"""
        
        buttons = [
            {'id': 'book_birthday', 'title': '🎉 Book Now'},
            {'id': 'birthday_gallery', 'title': '📸 Gallery'},
            {'id': 'main', 'title': '🏠 Main Menu'}
        ]
        
        return WhatsAppInteractiveMenu.create_button_message(text, buttons), 'birthday'
    
    def _get_wedding_menu(self) -> Tuple[Dict, str]:
        text = """💒 *WEDDING VENUE HIRE*

*PACKAGE PRICES:*
• Up to 100 guests: $1,250
• 100-200 guests: $2,250
• 200-300 guests: $3,000
• 300-400 guests: $3,500
• 400+ guests: $4,000

*INCLUDES:* Venue, Honeymoon Suite, 2 chalets, tables, chairs, parking, generator

*DEPOSIT:* $500 (non-refundable)
*PHOTO SHOOTS:* $100 + $5/person

Would you like a quote?"""
        
        buttons = [
            {'id': 'book_wedding', 'title': '💍 Get Quote'},
            {'id': 'wedding_menu', 'title': '🍽️ Catering'},
            {'id': 'main', 'title': '🏠 Main Menu'}
        ]
        
        return WhatsAppInteractiveMenu.create_button_message(text, buttons), 'wedding'
    
    def _get_conference_menu(self) -> Tuple[Dict, str]:
        text = """💼 *CONFERENCES & TEAM BUILDING*

*PRICES (per person):*
• Venue Only: $10
• Team Building Only: $10
• Full Package: $25

*FULL PACKAGE INCLUDES:*
✅ Half day team building
✅ Conference rooms
✅ Morning/afternoon teas
✅ 3 course meal

*CAPACITY:* Up to 1,000 people

Custom packages available!"""
        
        buttons = [
            {'id': 'book_conference', 'title': '📊 Book Event'},
            {'id': 'corporate_rates', 'title': '🏢 Group Rates'},
            {'id': 'main', 'title': '🏠 Main Menu'}
        ]
        
        return WhatsAppInteractiveMenu.create_button_message(text, buttons), 'conference'
    
    def _get_educational_menu(self) -> Tuple[Dict, str]:
        text = """📚 *EDUCATIONAL TOURS*

*SCHOOL PACKAGE:* $20/student

*INCLUDES:*
✅ Museum tour (Bushman paintings)
✅ 62 indigenous tree species
✅ 2 recreational activities
✅ Lunch & games zone access

*TEACHERS:* $3-5 each
*OVERNIGHT DORMS:* From $15/night
*TEAM BUILDING:* Available

Book your educational trip today!"""
        
        buttons = [
            {'id': 'book_school', 'title': '🏫 Book Trip'},
            {'id': 'curriculum', 'title': '📖 Curriculum'},
            {'id': 'main', 'title': '🏠 Main Menu'}
        ]
        
        return WhatsAppInteractiveMenu.create_button_message(text, buttons), 'educational'
    
    def _get_getaway_menu(self) -> Tuple[Dict, str]:
        text = """✈️ *GETAWAY PACKAGES* (2 people)

*3 NIGHTS:*
• Breakfast only: $184
• +Dinner: $256
• Full board: $304

*4 NIGHTS:*
• Breakfast only: $232
• +Dinner: $328
• Full board: $400

*INCLUDES:* Accommodation + 10 activities
💫 *SAVE UP TO 20%!*

Discounted rates for 4+ nights available"""
        
        buttons = [
            {'id': 'book_getaway', 'title': '🎫 Book Now'},
            {'id': 'package_details', 'title': '📋 Details'},
            {'id': 'main', 'title': '🏠 Main Menu'}
        ]
        
        return WhatsAppInteractiveMenu.create_button_message(text, buttons), 'getaway'
    
    def _get_location_menu(self) -> Tuple[Dict, str]:
        text = """📍 *LOCATION & DIRECTIONS*

*ADDRESS:*
625 Stephen Margolis Road (off Chitungwiza Road), Waterfalls, Harare

*FROM HARARE CBD:*
1. Simon Mazorodze Rd towards Harare South
2. Trabalas Exchange (2 exits left)
3. Follow Chitungwiza Rd past 2 Irvines
4. Left at Stephen Margolis Rd (look for billboard)
5. 1km to destination

⚠️ DON'T USE GOOGLE MAPS - follow our directions!

*PUBLIC TRANSPORT:* Kombi to DDF from Market Square, taxi from turnoff (50c)"""
        
        buttons = [
            {'id': 'share_location', 'title': '📍 Share Location'},
            {'id': 'contact_pickup', 'title': '🚗 Need Pickup?'},
            {'id': 'main', 'title': '🏠 Main Menu'}
        ]
        
        return WhatsAppInteractiveMenu.create_button_message(text, buttons), 'location'
    
    def _get_contact_menu(self) -> Tuple[Dict, str]:
        text = """📞 *CONTACT US*

*PHONE/WHATSAPP:*
+263779897192
+263775976333

*EMAIL:*
info@stephenmargolisresort.com

*OPENING HOURS:*
9am - 6pm daily

*OFFICE:*
Margolis Plaza, Room 205
(Cnr Harare Street & Speke Avenue)

We look forward to hearing from you!"""
        
        buttons = [
            {'id': 'call_now', 'title': '📱 Call Now'},
            {'id': 'whatsapp', 'title': '💬 WhatsApp'},
            {'id': 'main', 'title': '🏠 Main Menu'}
        ]
        
        return WhatsAppInteractiveMenu.create_button_message(text, buttons), 'contact'
    
    def process_interaction(self, user_id: str, interaction_id: str, sender_id: str = None, adapter=None) -> Tuple[Dict, str]:
        """Process button clicks and list selections"""
        
        if user_id not in self.user_sessions:
            self.user_sessions[user_id] = {'last_menu': 'main'}
        
        session = self.user_sessions[user_id]
        
        menu_mapping = {
            'activities': 'activities',
            'accommodation': 'accommodation',
            'restaurant': 'restaurant',
            'birthday': 'birthday',
            'wedding': 'wedding',
            'conference': 'conference',
            'educational': 'educational',
            'getaway': 'getaway',
            'location': 'location',
            'contact': 'contact',
            'main': 'main',
        }
        
        if interaction_id in menu_mapping:
            session['last_menu'] = menu_mapping[interaction_id]
            return self.get_menu(menu_mapping[interaction_id])
        
        # Handle action buttons
        action_response = self._handle_action_buttons(interaction_id, sender_id, adapter)
        
        if action_response:
            # Check if this is a template request
            if isinstance(action_response, dict) and action_response.get('type') == 'template':
                # Send template using the adapter
                if adapter:
                    template_result = adapter.send_template_message(
                        sender_id, 
                        action_response.get('template_name'), 
                        action_response.get('language', 'en')
                    )
                    if template_result.get('success'):
                        # Return a simple confirmation after template
                        text = """✅ *Booking Request Received!*

    Our team will contact you shortly to confirm your activity booking.

    📞 For immediate assistance: +263779897192

    Type MENU to return to main menu."""
                        return WhatsAppInteractiveMenu.create_text_message(text), session.get('last_menu', 'main')
                    else:
                        # Fallback to button message if template fails
                        text = """📅 *Activity Booking*

    Please reply with:
    • Activity name
    • Number of people
    • Preferred date

    Our team will confirm availability!

    Would you like to do anything else?"""
                        buttons = [
                            {'id': 'activities', 'title': '🎯 View Activities'},
                            {'id': 'book_activity', 'title': '📅 Book Another'},
                            {'id': 'main', 'title': '🏠 Main Menu'}
                        ]
                        return WhatsAppInteractiveMenu.create_button_message(text, buttons), session.get('last_menu', 'main')
            
            # Regular button or text message
            elif isinstance(action_response, dict) and action_response.get('type') == 'interactive':
                return action_response, session.get('last_menu', 'main')
            elif isinstance(action_response, dict):
                return action_response, session.get('last_menu', 'main')
        
        return self.get_menu('main')
    
    def _handle_action_buttons(self, action_id: str, sender_id: str = None, adapter=None) -> Optional[Dict]:
        """Handle action button clicks with text responses that include menus"""
        
        # For book_activity - return None to signal that we should send a template
        if action_id == 'book_activity':
            # Return a special marker to send template instead of button message
            return {'type': 'template', 'template_name': 'margolisactivitybooking', 'language': 'en'}
        
        # For ask_activities - return button message as before
        if action_id == 'ask_activities':
            text = """🎯 *More Activity Info*

    All activities are guided and include safety equipment. 
    Kids Play Area has swimming pool, swings, see-saw, play house, and jumping castle.

    Group discounts available for 10+ people!

    What would you like to do next?"""
            buttons = [
                {'id': 'book_activity', 'title': '📅 Book Activity'},
                {'id': 'activities', 'title': '🎯 Back to Activities'},
                {'id': 'main', 'title': '🏠 Main Menu'}
            ]
            return WhatsAppInteractiveMenu.create_button_message(text, buttons)
    
        
        # Keep all other actions as text responses
        actions = {
            'book_room': "📅 *Room Booking*\n\nPlease reply with:\n• Check-in date\n• Number of nights\n• Room type preference\n• Number of guests\n\nWe'll check availability and send payment details!",
            
            'room_amenities': "🏊 *Room Amenities*\n\n✅ Swimming pool access (accommodation guests only)\n✅ Free parking\n✅ Braai stands\n✅ Restaurant access\n✅ 24/7 security\n✅ Backup generator\n✅ Room service available",
            
            'order_food': "🍔 *Place Order*\n\nPlease reply with:\n• Item name and quantity\n• Any modifications\n• Room number or location\n\nOur kitchen is open 8am-9pm!",
            
            'full_menu': "📋 *Full Menu Available*\n\nContact us on WhatsApp for our complete menu with daily specials and chef recommendations!\n\n📞 +263779897192",
            
            'book_birthday': "🎂 *Birthday Booking*\n\nPlease reply with:\n• Date of event\n• Number of guests (min 5 paying)\n• Birthday person's name\n\nWe'll send you a quotation within 24 hours!",
            
            'birthday_gallery': "📸 *Birthday Gallery*\n\nContact us on WhatsApp (+263779897192) and we'll share photos of previous birthday celebrations at the resort!",
            
            'book_wedding': "💍 *Wedding Quote*\n\nPlease reply with:\n• Preferred wedding date\n• Estimated number of guests\n• Any specific requirements\n\nOur wedding coordinator will contact you within 24 hours!",
            
            'wedding_menu': "🍽️ *Wedding Catering*\n\nWe offer multiple catering options:\n• Buffet menu\n• Plated service\n• Traditional/Zimbabwean cuisine\n• Western menu\n• Vegetarian options\n\nContact us for full menu!",
            
            'book_conference': "📊 *Event Booking*\n\nPlease reply with:\n• Event date\n• Number of participants\n• Package preference\n• Any dietary requirements\n\nWe'll send a customized quotation!",
            
            'corporate_rates': "🏢 *Corporate Rates*\n\nSpecial rates for:\n• 20+ people: 10% discount\n• 50+ people: 15% discount\n• 100+ people: 20% discount\n• Annual contracts available\n\nContact us for a corporate proposal!",
            
            'book_school': "🏫 *School Trip Booking*\n\nPlease reply with:\n• School name\n• Number of students\n• Preferred date(s)\n• Grade level\n\nOur education coordinator will contact you!",
            
            'curriculum': "📖 *Curriculum Links*\n\nOur educational tours align with:\n• Heritage Studies\n• Environmental Science\n• Art & Culture\n• Physical Education\n• Social Studies\n\nContact us for detailed curriculum mapping!",
            
            'book_getaway': "🎫 *Getaway Booking*\n\nPlease reply with:\n• Number of nights (3 or 4)\n• Package type\n• Preferred dates\n\nWe'll confirm availability and send payment options!",
            
            'package_details': "📋 *Package Details*\n\nGetaway packages include:\n• Accommodation for 2 people\n• 10 shared activities\n• Daily breakfast (included)\n\nOptional add-ons:\n• Lunch ($15/day)\n• Dinner ($25/day)\n• Extra activities ($5 each)",
            
            'share_location': "📍 *Send Location*\n\nPlease share your current location using WhatsApp's location sharing feature, and we'll provide the best route to the resort!",
            
            'contact_pickup': "🚗 *Pickup Service*\n\nWe offer pickup service from:\n• Harare CBD: $10\n• Airport: $15\n• Any hotel in Harare: $10-20\n\nPlease share your location and preferred pickup time!",
            
            'call_now': "📱 *Call Us Directly*\n\n📞 +263779897192\n📞 +263775976333\n\nOur team is available 9am-6pm daily!",
            
            'whatsapp': "💬 *WhatsApp Us*\n\nClick this link to start a chat:\nhttps://wa.me/263779897192\n\nOr save our number: +263779897192"
        }
        
        result = actions.get(action_id)
        if result and isinstance(result, str):
            return WhatsAppInteractiveMenu.create_text_message(result)
        return result


    def handle_text_message(self, user_id: str, message: str) -> Tuple[Dict, str]:
        """Handle free text messages from users"""
        message_lower = message.lower().strip()
        
        # Special commands
        if message_lower in ['menu', 'main menu', 'start', 'hello', 'hi', 'hey', 'help']:
            return self.get_menu('main')
        
        # Keyword to menu mapping
        keyword_to_menu = {
            'activity': 'activities',
            'entrance': 'activities',
            'fee': 'activities',
            'room': 'accommodation',
            'stay': 'accommodation',
            'food': 'restaurant',
            'menu': 'restaurant',
            'eat': 'restaurant',
            'birthday': 'birthday',
            'party': 'birthday',
            'wedding': 'wedding',
            'marry': 'wedding',
            'conference': 'conference',
            'meeting': 'conference',
            'team building': 'conference',
            'school': 'educational',
            'tour': 'educational',
            'getaway': 'getaway',
            'package': 'getaway',
            'location': 'location',
            'direction': 'location',
            'address': 'location',
            'contact': 'contact',
            'phone': 'contact',
            'price': 'activities',
            'cost': 'activities',
        }
        
        for keyword, menu_id in keyword_to_menu.items():
            if keyword in message_lower:
                return self.get_menu(menu_id)
        
        # Default response
        default_text = """🤖 *I can help you with:*

• Activities & Entrance Fees
• Accommodation Booking
• Restaurant Menu
• Birthday Packages
• Weddings
• Conferences & Team Building
• Educational Tours
• Getaway Packages
• Location & Directions
• Contact Information

*Type "MENU" to see all options* or ask me anything about Stephen Margolis Resort!

📞 For immediate assistance: +263779897192"""
        
        return WhatsAppInteractiveMenu.create_text_message(default_text), 'main'