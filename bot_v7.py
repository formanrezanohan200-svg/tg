import logging
import os
import random
import uuid
import time
import sqlite3
import re
import asyncio 
from datetime import datetime, timedelta
from decimal import Decimal, getcontext 
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, error
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    filters,
    ConversationHandler,
)
from dotenv import load_dotenv 
import gspread
from google.oauth2.service_account import Credentials
# Import GSpreadException for general gspread errors
from gspread.exceptions import WorksheetNotFound, APIError 

# Set precision high enough for high-precision payments (up to 6 decimals)
getcontext().prec = 10 
load_dotenv()

# --- BOT TOKENS & CONFIGURATION ---
BOT_TOKEN = os.getenv("BOT_TOKEN", "8512286554:AAGWOVs8092uLcz7gCGrWgEMWHd1fwAqKI") 
ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID", "6807772148") # Admin ID
PAYMENT_WALLET_NUMBER = os.getenv("PAYMENT_WALLET_NUMBER", "777904898") 

# --- GOOGLE SHEETS CONFIGURATION ---
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
CREDENTIALS_FILE = 'credentials.json'
YOUR_MAIN_ALPHANUMERIC_SPREADSHEET_ID = os.getenv("SPREADSHEET_ID", "1laUFa9ApUV8zVAGzMq11GzvZ7Pr--G_R2IfegVEibpc")

SHEET_CONFIG = {
    "BM": {
        "1_PAGE_BM": {
            "sheet_id": YOUR_MAIN_ALPHANUMERIC_SPREADSHEET_ID,
            "sheet_name": "1 page 1 BM",
            "data_col": "A", "status_col": "B", "name_col": "C", "country_col": "D", "whatsapp_col": "E"
        },
        "REGULAR_BM": {
            "sheet_id": YOUR_MAIN_ALPHANUMERIC_SPREADSHEET_ID,
            "sheet_name": "Regular BM",
            "data_col": "A", "status_col": "B", "name_col": "C", "country_col": "D", "whatsapp_col": "E"
        },
        "SHOPIFY_BM": {
            "sheet_id": YOUR_MAIN_ALPHANUMERIC_SPREADSHEET_ID,
            "sheet_name": "Shopify BM",
            "data_col": "A", "status_col": "B", "name_col": "C", "country_col": "D", "whatsapp_col": "E"
        },
        # NOTE: This name must EXACTLY match the tab name in your Google Sheet!
        "VERIFIED_BM": {
            "sheet_id": YOUR_MAIN_ALPHANUMERIC_SPREADSHEET_ID,
            "sheet_name": "Verified BM", 
            "data_col": "A", "status_col": "B", "name_col": "C", "country_col": "D", "whatsapp_col": "E"
        }
    }
}

PRODUCTS = {
    "BM": {
        "1_PAGE_BM": {"name": "1 Page 1 BM", "price": 0.20, "stock": 0},
        "REGULAR_BM": {"name": "Regular BM", "price": 0.10, "stock": 0},
        "SHOPIFY_BM": {"name": "Shopify BM", "price": 0.25, "stock": 0},
        "VERIFIED_BM": {"name": "Verified BM", "price": 10.00, "stock": 0},
    },
    "NEXTDOOR": {
        "USA": {"name": "Nextdoor USA", "price": 2.00, "stock": 200},
        "UK": {"name": "Nextdoor UK", "price": 2.00, "stock": 150},
        "CANADA": {"name": "Nextdoor Canada", "price": 2.00, "stock": 150},
        "AUSTRALIA": {"name": "Nextdoor Australia", "price": 2.00, "stock": 100},
        "GERMANY": {"name": "Nextdoor Germany", "price": 2.00, "stock": 100},
    }
}

# --- STATE DEFINITIONS & GLOBAL TRACKING ---
HOME, SIGNUP_NAME, SIGNUP_COUNTRY, SIGNUP_CONTACT, SELECT_PRODUCT_TYPE, SELECT_PRODUCT, ASK_QUANTITY, CONFIRM_ORDER, AWAITING_TX_ID = range(9)
ADMIN_PANEL, ADMIN_AWAITING_PRICE = range(9, 11) 
ORDER_DATABASE = {}

# --- PAYMENT DATABASE ---
DB_PATH = 'payments.db'
payment_db_conn = sqlite3.connect(DB_PATH, check_same_thread=False)
payment_db_cursor = payment_db_conn.cursor()

payment_db_cursor.execute('''
    CREATE TABLE IF NOT EXISTS pending_payments (
        order_id TEXT PRIMARY KEY,
        chat_id INTEGER,
        expected_amount REAL,
        unique_amount TEXT, 
        payment_code TEXT,
        created_at TIMESTAMP,
        status TEXT DEFAULT 'pending'
    )
''')
payment_db_cursor.execute('''
    CREATE TABLE IF NOT EXISTS received_payments (
        unique_amount TEXT PRIMARY KEY,
        received_time TIMESTAMP,
        status TEXT DEFAULT 'unmatched'
    )
''')
payment_db_conn.commit()

# --- SETUP LOGGING ---
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO
)
logger = logging.getLogger(__name__)

# Regex to capture the amount and time from the payment confirmation message
PAYMENT_RECEIVE_REGEX = re.compile(
    r"Amount:\s*([\d.]+)\s*(USDT|USD|BUSD)\s*.*?"
    r"Date\s*&\s*Time:\s*(\d{4}-\d{2}-\d{2}\s*\d{2}:\d{2}:\d{2})\s*\(UTC\)",
    re.DOTALL | re.IGNORECASE
)

# --- GLOBAL ADVANCED ERROR HANDLER ---
async def error_handler(update: object, context):
    """Log the error and notify the admin of the failure."""
    
    # Log the full exception traceback for debugging
    logger.error("Exception while handling an update:", exc_info=context.error)
    
    error = context.error
    admin_error_message = f"🚨 **GLOBAL BOT ERROR**\n\n"
    admin_error_message += f"**Time:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}\n"
    
    # Try to identify the user and input that caused the error
    if update and hasattr(update, 'effective_user') and update.effective_user:
        admin_error_message += f"**User:** `{update.effective_user.id}` ({update.effective_user.first_name})\n"
    if update and hasattr(update, 'callback_query') and update.callback_query:
        admin_error_message += f"**Action Data:** `{update.callback_query.data}`\n"
    elif update and hasattr(update, 'message') and update.message and update.message.text:
        admin_error_message += f"**Message Input:** `{update.message.text}`\n"
    
    # Format the error details
    admin_error_message += f"**Error Type:** `{type(error).__name__}`\n"
    admin_error_message += f"**Error Message:** `{str(error)[:300]}...`\n"

    # Notify the admin
    try:
        await context.bot.send_message(
            chat_id=ADMIN_CHAT_ID,
            text=admin_error_message,
            parse_mode='Markdown'
        )
    except Exception as e:
        logger.error(f"Failed to send error notification to admin: {e}")

    # Optionally, notify the user (only if not the admin)
    if update and hasattr(update, 'effective_user') and update.effective_user and str(update.effective_user.id) != ADMIN_CHAT_ID:
        user_message = (
            "⚠️ **System Error**\n\n"
            "An unexpected error occurred while processing your request. "
            "The issue has been logged and the administrator has been notified. "
            "Please try again or type /start."
        )
        try:
            if update.effective_message:
                await update.effective_message.reply_text(user_message, parse_mode='Markdown')
        except Exception:
             logger.warning("Could not send error message to user.")

# --- HIGH-PRECISION PAYMENT GENERATION HELPER (FIXED AND EXPANDED) ---
def get_unique_fractional_part(base_amount_dec):
    """
    Generates a unique fractional part (1-99 micro-USD) that hasn't been used 
    in any currently pending customer order or unmatched received payment.
    """
    
    # Query both tables to find unique amounts that are currently "live"
    payment_db_cursor.execute('''
        SELECT unique_amount FROM pending_payments WHERE status = 'pending'
        UNION
        SELECT unique_amount FROM received_payments WHERE status = 'unmatched'
    ''')
    
    recent_unique_fractions = set()
    for row in payment_db_cursor.fetchall():
        try:
            unique_amount_dec = Decimal(row[0]) 
            # Isolates the fractional part beyond the two standard decimals
            # Quantize to 6 decimal places (micro-dollars)
            fractional_part = (unique_amount_dec - unique_amount_dec.quantize(Decimal('0.00'))).quantize(Decimal('1E-6'))
            if fractional_part > Decimal('0.000000'):
                 recent_unique_fractions.add(fractional_part)
        except Exception as e:
            # We must be robust to parsing a string that might not be a valid Decimal (e.g., if we fall back to manual entry)
            logger.warning(f"Failed to parse Decimal from DB: {row[0]}. Error: {e}")
            
    base_amount_dec_rounded = base_amount_dec.quantize(Decimal('0.00')) 
    
    # Use a random sequence of 1 to 99 micro-dollars (0.000001 to 0.000099)
    possible_fractions = [Decimal(i) * Decimal('1E-6') for i in range(1, 100)]
    random.shuffle(possible_fractions) # Randomize the search order
    
    for amount_extra_dec in possible_fractions:
        if amount_extra_dec not in recent_unique_fractions:
            # Found a unique fraction!
            return base_amount_dec_rounded + amount_extra_dec
            
    # Fallback to base amount if all 99 unique fractions are in use 
    logger.error("All 99 unique fractional parts are currently in use. Falling back to base amount.")
    return base_amount_dec_rounded

# --- FIX: Ensure unique_amount is stored as a fixed 6-decimal string for exact string matching ---
def store_payment_data(order_id, chat_id, expected_amount):
    """Generates and stores the unique payment amount as a fixed 6-decimal string."""
    base_amount_dec = Decimal(str(expected_amount))
    try:
        unique_amount_dec = get_unique_fractional_part(base_amount_dec)
    except Exception as e:
        logger.error(f"Error generating unique amount for {order_id}: {e}. Falling back to base amount.")
        unique_amount_dec = base_amount_dec.quantize(Decimal('0.00'))
    
    # --- CRITICAL FIX 1: Store the full 6-decimal string for exact matching ---
    # This string will be used for database comparison, it must be the full length
    unique_amount_str = f"{unique_amount_dec:.6f}" 
    
    payment_db_cursor.execute('''
        INSERT OR REPLACE INTO pending_payments 
        (order_id, chat_id, expected_amount, unique_amount, payment_code, created_at)
        VALUES (?, ?, ?, ?, ?, datetime('now'))
    ''', (order_id, chat_id, expected_amount, unique_amount_str, f"PAY{random.randint(1000, 9999)}"))
    payment_db_conn.commit()
    
    logger.info(f"Order {order_id} created. Unique amount stored: '{unique_amount_str}'")
    
    return {'unique_amount': unique_amount_str} 

# --- MODIFIED: Uses the full 6-decimal string and strips it ONLY for display/copying ---
def create_manual_payment_instructions(order_id, total_price, unique_amount_full_str):
    """Creates the payment instructions, correctly formatting the unique amount for client copying."""
    
    # --- CRITICAL FIX 2: Re-strip the full 6-decimal string for the cleanest display ---
    copyable_amount_and_currency = unique_amount_full_str.rstrip('0').rstrip('.')
    
    instructions = f"""
💳 **BINANCE PAY - AUTO-VERIFICATION SYSTEM** ⏳
*Verification is done by matching your unique amount to the admin's received data.*

📋 **Order ID**: `{order_id}`
💰 **Base Amount**: **${total_price:.2f}**

---
**👤 RECEIVER ID (UID) (Tap to Copy):**
`{PAYMENT_WALLET_NUMBER}`

🎯 **PAYMENT METHOD:**

**✅ EXACT UNIQUE AMOUNT (Tap to Copy):**

`{copyable_amount_and_currency}`

*(Send **EXACTLY** this amount for automated verification)*

---
2. Reply to this message with your **Transaction ID (TX ID)** immediately after sending.

⏰ **Verification**: Automated check runs as soon as admin sends the received payment data.
"""
    return instructions

# --- FULFILLMENT LOGIC ---
async def fulfill_order_process_auto(order_id, chat_id, unique_amount_paid, bot):
    order = ORDER_DATABASE.get(order_id)
    if not order:
        logger.error(f"Fulfillment failed: Order {order_id} not found in database.")
        return
    
    # Update status and payment tracking
    order['status'] = 'COMPLETED'
    quantity = order['quantity']
    
    # Update received_payments status to 'matched' to prevent future duplicate errors
    payment_db_cursor.execute('UPDATE received_payments SET status = ? WHERE unique_amount = ?', ('matched', unique_amount_paid))
    payment_db_conn.commit()
    
    payment_db_cursor.execute('UPDATE pending_payments SET status = ? WHERE order_id = ?', ('completed_auto', order_id))
    payment_db_conn.commit()
    
    # Buyer info for Google Sheet update
    buyer_info = {
        'name': order.get('buyer_name', 'Unknown'),
        'country': order.get('buyer_country', 'Unknown'),
        'whatsapp': order.get('buyer_whatsapp', 'Unknown')
    }
    
    product_key = order['product_key']
    product_type, specific_key = product_key.split('_', 1)
    fulfillment_data = None
    
    # Handle BM fulfillment (Synchronous sheet access)
    if product_type == "BM" and specific_key:
        # Note: This is synchronous, which is fine for fulfillment but blocks the loop. 
        # In high-traffic apps, this should be offloaded to a thread/executor.
        bm_data_list = fulfill_bm_order(specific_key, quantity, buyer_info) 
        if bm_data_list:
            # Update local stock count (will be re-updated on next load_initial_stock call)
            if specific_key in PRODUCTS['BM']:
                PRODUCTS['BM'][specific_key]['stock'] -= quantity
            delivery_parts = [f"**{order['details']['name']} {i}:**\n`{bm_data}`\n" for i, bm_data in enumerate(bm_data_list, 1)]
            fulfillment_data = "\n".join(delivery_parts)
        else:
            fulfillment_data = "❌ **Error: Could not fetch BM data.** Please contact administrator."
    # Handle other products (Placeholder delivery)
    else:
        if product_type == "NEXTDOOR" and specific_key in PRODUCTS['NEXTDOOR']:
            PRODUCTS['NEXTDOOR'][specific_key]['stock'] -= quantity
        fulfillment_data = f"Product keys for {order['details']['name']} x{quantity} sent here. (Placeholder)"
    
    # Send message to customer
    customer_message = (
        f"🎉 **PAYMENT CONFIRMED! ORDER COMPLETE!** ✅\n"
        f"--- \n"
        f"**Order ID**: `{order['order_id']}`\n"
        f"**Amount Paid**: ${unique_amount_paid.rstrip('0').rstrip('.')} (Auto-Verified)\n" 
        f"--- \n"
        f"📦 **DELIVERY DETAILS:**\n{fulfillment_data}\n\n"
        "Thank you for your purchase! 🛍️"
    )
    
    try:
        await bot.send_message(chat_id=chat_id, text=customer_message, parse_mode='Markdown')
        logger.info(f"Order {order_id} successfully fulfilled automatically.")
    except error.BadRequest as e:
        logger.error(f"Failed to send message to user {chat_id} for order {order_id}: {e}")
        # Notify admin if customer blocked the bot
        await bot.send_message(
            chat_id=ADMIN_CHAT_ID,
            text=f"⚠️ **DELIVERY FAILED:** Could not send fulfillment message to customer `{chat_id}` for order `{order_id}`. Customer may have blocked the bot. Data was: \n\n{fulfillment_data}",
            parse_mode='Markdown'
        )

# --- ADMIN HANDLERS ---
# ... (admin_panel_start, set_product_price_prompt, process_new_price, view_order_statistics, view_pending_orders, receive_data_info are unchanged)

async def admin_panel_start(update: Update, context):
    query = update.callback_query
    if query: await query.answer()
    effective_message = query.message if query else update.message
    
    if str(update.effective_user.id) != ADMIN_CHAT_ID:
        await effective_message.reply_text("❌ Unauthorized access to admin panel.")
        return HOME

    keyboard = [
        [InlineKeyboardButton("⚙️ Set Product Price", callback_data="ADMIN_SET_PRICE_PROMPT")],
        [InlineKeyboardButton("📊 Order Statistics", callback_data="ADMIN_VIEW_STATS")],
        [InlineKeyboardButton("💰 Receive Payment Data Info", callback_data="ADMIN_RECEIVE_DATA_INFO")],
        [InlineKeyboardButton("📝 View Pending Orders", callback_data="ADMIN_VIEW_PENDING")],
        [InlineKeyboardButton("⬅️ Back to Start", callback_data="ACTION_START")],
    ]
    
    text = "🛠️ **Admin Panel**\n\nWelcome, Administrator. Select an action:"
    
    if query:
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
    else:
        await effective_message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        
    return ADMIN_PANEL

async def set_product_price_prompt(update: Update, context):
    query = update.callback_query
    await query.answer()
    
    products_list = []
    for p_type, products in PRODUCTS.items():
        for p_key, details in products.items():
            full_key = f"{p_type}_{p_key}"
            products_list.append(f"`{full_key}`: {details['name']} (${details['price']:.2f})")
    
    text = (
        "💡 **Set Price**\n\n"
        "Send the new price and product key in this format:\n"
        "`<PRODUCT_KEY> <NEW_PRICE>`\n\n"
        "**Example:** `BM_REGULAR_BM 0.15`\n\n"
        "**Available Product Keys:**\n" + "\n".join(products_list) +
        "\n\nType **cancel** to return to the Admin Panel."
    )
    
    await query.edit_message_text(text, parse_mode='Markdown')
    return ADMIN_AWAITING_PRICE

async def process_new_price(update: Update, context):
    input_text = update.message.text.strip()
    
    if input_text.upper() == "CANCEL":
        await update.message.reply_text("❌ Price setting cancelled.")
        return await admin_panel_start(update, context) 
        
    try:
        parts = input_text.split()
        if len(parts) != 2:
            raise ValueError("Incorrect format. Expected: <PRODUCT_KEY> <NEW_PRICE>")
            
        product_key = parts[0].upper()
        new_price = Decimal(parts[1])

        # Validate product key
        product_type, specific_key = product_key.split('_', 1)
        
        if product_type in PRODUCTS and specific_key in PRODUCTS.get(product_type, {}):
            PRODUCTS[product_type][specific_key]['price'] = float(new_price)
            await update.message.reply_text(
                f"✅ **Price Updated!**\n\n"
                f"**Product**: {PRODUCTS[product_type][specific_key]['name']}\n"
                f"**New Price**: ${new_price:.2f}",
                parse_mode='Markdown'
            )
        else:
            await update.message.reply_text(f"❌ **Error:** Product key `{product_key}` not found.")
            return ADMIN_AWAITING_PRICE 
            
    except Exception as e:
        await update.message.reply_text(
            f"❌ **Input Failed.** Error: {e}. Please use the format: `<PRODUCT_KEY> <NEW_PRICE>`",
            parse_mode='Markdown'
        )
        return ADMIN_AWAITING_PRICE 
        
    return await admin_panel_start(update, context)

async def view_order_statistics(update: Update, context):
    query = update.callback_query
    await query.answer()

    stats_periods = {
        "1 Day": 1,
        "7 Days": 7,
        "30 Days": 30
    }
    
    stats_output = "📊 **Order Statistics (Completed Orders)**\n"
    stats_output += "--- \n"
    
    for label, days in stats_periods.items():
        time_filter = f"datetime('now', '-{days} day')"
        
        payment_db_cursor.execute(f'''
            SELECT COUNT(order_id), SUM(expected_amount)
            FROM pending_payments 
            WHERE status = 'completed_auto' AND datetime(created_at) >= {time_filter}
        ''')
        
        count, earnings = payment_db_cursor.fetchone()
        
        total_orders = int(count) if count else 0
        total_earnings = Decimal(str(earnings)) if earnings else Decimal('0.00') 
        
        stats_output += f"📅 **Last {label}:**\n"
        stats_output += f"  - **Orders Complete**: {total_orders}\n"
        stats_output += f"  - **Total Earned**: ${total_earnings:.2f}\n"
        stats_output += "--- \n"

    keyboard = [[InlineKeyboardButton("⬅️ Back to Admin Panel", callback_data="ADMIN_PANEL_START")]]
    await query.edit_message_text(stats_output, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
    
    return ADMIN_PANEL

async def view_pending_orders(update: Update, context):
    query = update.callback_query
    await query.answer()
    
    payment_db_cursor.execute('SELECT order_id, unique_amount, created_at FROM pending_payments WHERE status = ?', ('pending',))
    pending_orders = payment_db_cursor.fetchall()

    if not pending_orders:
        message = "✅ No customer orders are currently awaiting payment verification."
    else:
        message = "📋 **PENDING CUSTOMER ORDERS** (Awaiting Verification):\n\n"
        for order_id, unique_amount, created_at in pending_orders:
            # Display a clean version of the amount for the admin
            clean_amount = unique_amount.rstrip('0').rstrip('.')
            message += f"**Order ID**: `{order_id}`\n"
            message += f"**Unique Amount**: `{clean_amount}` (Search value is `{unique_amount}`)\n"
            message += f"**Created**: {created_at.split(' ')[1].split('.')[0]} (UTC)\n\n"
    
    keyboard = [[InlineKeyboardButton("⬅️ Back to Admin Panel", callback_data="ADMIN_PANEL_START")]]
    await query.edit_message_text(message, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
    return ADMIN_PANEL

async def receive_data_info(update: Update, context):
    query = update.callback_query
    await query.answer()
    
    text = (
        "💡 **Payment Data Submission**\n\n"
        "To verify a customer payment, copy and paste the full payment email/log into this chat.\n\n"
        "**Required Format Example:**\n"
        "```\n"
        "💰 NEW CRYPTO PAYMENT RECEIVED! 💰\n\n"
        "Status: Successful\n"
        "Amount: 0.100001 USDT\n"
        "Date & Time: 2025-11-08 18:22:52 (UTC)\n"
        "Time Source: Transaction Time (UTC)\n"
        "Source Email Subject: Binance Payment Receive Successful - 2025-11-08 18:22:52 (UTC)\n"
        "```\n\n"
        "The bot will automatically parse the **Amount** and attempt to match it with a pending customer order."
    )
    
    keyboard = [[InlineKeyboardButton("⬅️ Back to Admin Panel", callback_data="ADMIN_PANEL_START")]]
    await query.edit_message_text(text, parse_mode='Markdown')
    return ADMIN_PANEL

async def process_admin_received_data(update: Update, context):
    if str(update.effective_user.id) != ADMIN_CHAT_ID: return
    
    text = update.message.text
    match = PAYMENT_RECEIVE_REGEX.search(text)
    
    if match:
        amount_str = match.group(1).strip()
        logger.info(f"Admin message parsed raw amount string: '{amount_str}'") # DEBUG LOG: RAW PARSED AMOUNT
        
        try:
            # Parse amount to high-precision Decimal and quantize to 6 decimals
            amount_dec = Decimal(amount_str).quantize(Decimal('1E-6'))
            
            # --- CRITICAL FIX 3: Normalize to the fixed 6-decimal string for DB search ---
            unique_amount = f"{amount_dec:.6f}"
            
            received_time = match.group(3)
            logger.info(f"Admin message normalized unique amount for DB/match: '{unique_amount}'") # DEBUG LOG: NORMALIZED AMOUNT
        except Exception as e:
            logger.error(f"Failed to parse amount {amount_str}: {e}")
            await update.message.reply_text("❌ **Error Parsing Amount:** The amount could not be processed as a high-precision number.", parse_mode='Markdown')
            return

        try:
            # Store the received payment data
            payment_db_cursor.execute('''
                INSERT INTO received_payments (unique_amount, received_time, status)
                VALUES (?, ?, ?)
            ''', (unique_amount, received_time, 'unmatched'))
            payment_db_conn.commit()
            
            await update.message.reply_text(
                f"✅ **Payment Data Stored!**\n"
                f"**Amount**: `{unique_amount.rstrip('0').rstrip('.')} USD`\n" # Display clean amount
                f"**Time**: {received_time} (UTC)\n\n"
                f"Attempting to **auto-verify** against pending customer orders now...",
                parse_mode='Markdown'
            )
            
            # Attempt to match and fulfill
            await auto_verify_and_fulfill(context.bot, unique_amount)

        except sqlite3.IntegrityError:
            # This error means the unique_amount is already in the received_payments table
            await update.message.reply_text(f"⚠️ **Duplicate Payment Received!** Amount `{unique_amount.rstrip('0').rstrip('.')}` already processed or stored. This amount may have been used by another order or submitted twice.", parse_mode='Markdown')
        except Exception as e:
            await update.message.reply_text(f"❌ **Database Error** while storing payment data: {e}", parse_mode='Markdown')
            
    else:
        if "amount:" in text.lower() or "usdt" in text.lower() or "binance" in text.lower():
            await update.message.reply_text(
                "❌ **Parsing Failed.** Please ensure the entire payment message is copied exactly, including 'Amount:' and 'Date & Time:', and that the amount is present.",
                parse_mode='Markdown'
            )

async def auto_verify_and_fulfill(bot, received_unique_amount):
    logger.info(f"Auto-Verification: Searching for pending order with unique_amount='{received_unique_amount}'") # DEBUG LOG: SEARCH START
    
    payment_db_cursor.execute('''
        SELECT order_id, chat_id
        FROM pending_payments 
        WHERE unique_amount = ? AND status = ?
    ''', (received_unique_amount, 'pending'))
    
    match = payment_db_cursor.fetchone()
    
    if match:
        order_id, chat_id = match
        logger.info(f"Auto-Verification: MATCH FOUND for order_id='{order_id}'") # DEBUG LOG: MATCH FOUND
        await fulfill_order_process_auto(order_id, chat_id, received_unique_amount, bot)
        await bot.send_message(
            chat_id=ADMIN_CHAT_ID,
            text=f"🔥 **AUTO-MATCH SUCCESS!** 🔥\n"
                 f"**Customer Order ID**: `{order_id}`\n"
                 f"**Amount Matched**: `{received_unique_amount.rstrip('0').rstrip('.')}`\n\n"
                 f"The order has been automatically fulfilled and the customer notified.",
            parse_mode='Markdown'
        )
    else:
        logger.info(f"Auto-Verification: NO MATCH FOUND for unique_amount='{received_unique_amount}'") # DEBUG LOG: NO MATCH
        await bot.send_message(
            chat_id=ADMIN_CHAT_ID,
            text=f"⏳ **Payment Unmatched.**\n"
                 f"Amount `{received_unique_amount.rstrip('0').rstrip('.')}` has been recorded, but no pending customer order matches this unique amount.",
            parse_mode='Markdown'
        )

# --- GOOGLE SHEETS FUNCTIONS ---
# ... (get_google_sheet_client, col_to_num, get_available_stock_count, load_initial_stock, get_available_bm_data, mark_bm_as_sold, fulfill_bm_order are unchanged)

def get_google_sheet_client():
    try:
        creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPES)
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        logger.error(f"Failed to authenticate Google Sheets with credentials.json: {e}")
        return None

def col_to_num(col_letter):
    if not col_letter: return 0
    return ord(col_letter.upper()) - ord('A') + 1

def get_available_stock_count(product_type):
    sheet_config = None
    worksheet_name = "N/A"
    try:
        client = get_google_sheet_client()
        if not client: return 0
        sheet_config = SHEET_CONFIG["BM"].get(product_type)
        if not sheet_config: return 0
        
        spreadsheet = client.open_by_key(sheet_config["sheet_id"])
        
        worksheet_name = sheet_config["sheet_name"]
        worksheet = spreadsheet.worksheet(worksheet_name)
        
        data_col = sheet_config['data_col']
        status_col = sheet_config['status_col']
        range_label = f"{data_col}2:{status_col}1000"
        all_data = worksheet.get_values(range_label)
        available_count = 0
        for row in all_data:
            bm_data = row[0].strip() if len(row) > 0 and row[0] else ""
            current_status = row[1].strip() if len(row) > 1 and row[1] else ""
            # Count if data exists and status is empty or 'Available'
            if bm_data and current_status in ["", "Available"]:
                available_count += 1
        return available_count
    except gspread.WorksheetNotFound as e:
        # CRITICAL ERROR HANDLING: Log the exact error and return 0 stock to prevent bot crash
        logger.error(f"Error: Google Sheet Worksheet '{worksheet_name}' not found for product '{product_type}'. Check name/permissions. Error: {e}")
        return 0
    except Exception as e:
        # General connection or data parsing error
        logger.error(f"General error counting stock for {product_type}. Details: {e}")
        return 0

def load_initial_stock():
    """Fetches stock counts for all BM products from Google Sheets on startup."""
    for key, details in PRODUCTS["BM"].items():
        count = get_available_stock_count(key)
        details["stock"] = count

def get_available_bm_data(product_type, quantity):
    try:
        client = get_google_sheet_client()
        if not client: return None
        sheet_config = SHEET_CONFIG["BM"].get(product_type)
        if not sheet_config: return None
        spreadsheet = client.open_by_key(sheet_config["sheet_id"])
        worksheet = spreadsheet.worksheet(sheet_config["sheet_name"])
        data_col = sheet_config['data_col']
        col_end_letter = sheet_config['whatsapp_col']
        range_label = f"{data_col}2:{col_end_letter}1000"
        all_rows_data = worksheet.get_values(range_label)
        available_bms = []
        for i, row in enumerate(all_rows_data, start=2):
            bm_data = row[0].strip() if len(row) > 0 and row[0] else ""
            current_status = row[1].strip() if len(row) > 1 and row[1] else ""
            if bm_data and current_status in ["", "Available"]:
                available_bms.append({'row': i, 'data': bm_data})
            if len(available_bms) >= quantity: break
        return available_bms if len(available_bms) >= quantity else None
    except Exception as e:
        logger.error(f"Error fetching BM data from Google Sheets: {e}")
        return None

def mark_bm_as_sold(worksheet, bm_info, buyer_info, sheet_config):
    try:
        row_num = bm_info['row']
        # Update columns based on config
        worksheet.update_cell(row_num, col_to_num(sheet_config['status_col']), "Sold")
        worksheet.update_cell(row_num, col_to_num(sheet_config['name_col']), buyer_info['name'])
        worksheet.update_cell(row_num, col_to_num(sheet_config['country_col']), buyer_info['country'])
        worksheet.update_cell(row_num, col_to_num(sheet_config['whatsapp_col']), buyer_info['whatsapp'])
        return True
    except Exception as e:
        logger.error(f"Error marking BM as sold: {e}")
        return False

def fulfill_bm_order(product_type, quantity, buyer_info):
    try:
        client = get_google_sheet_client()
        if not client: return None
        sheet_config = SHEET_CONFIG["BM"].get(product_type)
        if not sheet_config: return None
        spreadsheet = client.open_by_key(sheet_config["sheet_id"])
        worksheet = spreadsheet.worksheet(sheet_config["sheet_name"])
        
        available_bms = get_available_bm_data(product_type, quantity)
        if not available_bms:
            # Re-load stock in case it just sold out
            load_initial_stock() 
            return None
            
        bm_data_list = []
        successful_updates = 0
        
        for bm_info in available_bms:
            if mark_bm_as_sold(worksheet, bm_info, buyer_info, sheet_config):
                bm_data_list.append(bm_info['data'])
                successful_updates += 1
                if successful_updates >= quantity: break
                
        # Update local stock count after fulfillment attempt
        load_initial_stock()
        
        return bm_data_list if successful_updates == quantity else None
        
    except Exception as e:
        logger.error(f"Error fulfilling BM order: {e}")
        return None

def get_product_details(product_key):
    parts = product_key.split('_', 1)
    if len(parts) < 2: return None
    product_type, specific_key = parts[0], parts[1]
    if product_type in PRODUCTS and specific_key in PRODUCTS[product_type]:
        details = PRODUCTS[product_type][specific_key]
        details['type'] = product_type
        details['key'] = product_key
        return details
    return None

def generate_order_id():
    return str(uuid.uuid4())[:8].upper()

# --- CLIENT HANDLERS ---
# ... (signup_start, signup_name, signup_country, signup_contact, buy_start, select_product_list, ask_quantity, process_quantity_and_confirm are unchanged)

async def signup_start(update: Update, context):
    await update.callback_query.answer()
    await update.callback_query.edit_message_text("📝 **User Registration**\n\nPlease enter your **Full Name** to register your contact details:", parse_mode='Markdown')
    return SIGNUP_NAME

async def signup_name(update: Update, context):
    context.user_data['temp_name'] = update.message.text.strip()
    await update.message.reply_text("🗺️ Thanks! Now, please enter your **Country/Location**:", parse_mode='Markdown')
    return SIGNUP_COUNTRY

async def signup_country(update: Update, context):
    context.user_data['temp_country'] = update.message.text.strip()
    await update.message.reply_text("📞 Great! Finally, please enter your **WhatsApp Contact Number** (for support/delivery):", parse_mode='Markdown')
    return SIGNUP_CONTACT

async def signup_contact(update: Update, context):
    contact = update.message.text.strip()
    context.user_data['contract_info'] = {
        'name': context.user_data.pop('temp_name'),
        'country': context.user_data.pop('temp_country'),
        'contact': contact
    }
    keyboard = [[InlineKeyboardButton("🛒 Continue to Shop", callback_data="ACTION_BUY")]]
    await update.message.reply_text(
        f"✅ **Registration Complete!**\n\n**Name**: {context.user_data['contract_info']['name']}\n**Contact**: {contact}",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )
    return HOME

async def buy_start(update: Update, context):
    if update.callback_query: await update.callback_query.answer()
    contract_info = context.user_data.get('contract_info')
    if not contract_info or not contract_info.get('name'):
        keyboard = [[InlineKeyboardButton("📝 Sign Up Now", callback_data="ACTION_SIGNUP")]]
        await (update.callback_query.edit_message_text if update.callback_query else update.effective_message.reply_text)(
            "⚠️ **Registration Required**\n\nYou must **Sign Up** (register your contact info) before purchasing.",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )
        return HOME
    
    # Reload stock counts to ensure they are current before displaying the menu
    load_initial_stock() 
    
    keyboard = [
        [InlineKeyboardButton("💳 BM Packages", callback_data="TYPE_BM")],
        [InlineKeyboardButton("🏘️ Nextdoor Accounts", callback_data="TYPE_NEXTDOOR")],
        [InlineKeyboardButton("🏠 Back to Home", callback_data="ACTION_START")]
    ]
    text = "🛍️ **Select a Product Category**:"
    
    if update.callback_query:
        try:
            # FIX: Wrap in try/except to ignore "Message is not modified" error 
            await update.callback_query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
        except error.BadRequest as e:
            # Only ignore the "Message is not modified" error, re-raise others
            if "Message is not modified" not in str(e):
                raise e
    else:
        await update.effective_message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
    return SELECT_PRODUCT_TYPE

async def select_product_list(update: Update, context):
    query = update.callback_query
    await query.answer()
    product_type = query.data.split('_')[1]
    context.user_data['product_type'] = product_type
    
    products_data = PRODUCTS.get(product_type, {})
    keyboard = []
    
    for key, details in products_data.items():
        product_key = f"{product_type}_{key}"
        stock = details.get("stock", 0)
        stock_text = f"({stock} in stock)" if stock > 0 else "🛑 (OUT OF STOCK)"
        button_text = f"{details['name']} - ${details['price']:.2f} {stock_text}" 
        
        # Only allow selection if stock > 0
        callback_data = f"PRODUCT_{product_key}" if stock > 0 else "NO_STOCK"
        keyboard.append([InlineKeyboardButton(button_text, callback_data=callback_data)])
        
    keyboard.append([InlineKeyboardButton("⬅️ Back to Categories", callback_data="ACTION_BUY")])
    
    await query.edit_message_text(
        f"📋 **Select a {product_type} Product**:",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )
    return SELECT_PRODUCT

async def ask_quantity(update: Update, context):
    query = update.callback_query
    await query.answer()
    
    if query.data == "NO_STOCK":
        await query.answer("🛑 This item is currently out of stock.", show_alert=True)
        return SELECT_PRODUCT
        
    product_key = query.data.split('PRODUCT_')[1]
    details = get_product_details(product_key)
    
    if not details or details.get('stock', 0) <= 0:
        await query.edit_message_text("❌ This product is currently out of stock. Please select another item.", parse_mode='Markdown')
        return await buy_start(update, context) 
        
    context.user_data['selected_product_key'] = product_key
    await query.edit_message_text(
        f"**Selected:** {details['name']} - **${details['price']:.2f}**\n"
        f"**Stock Available:** {details['stock']}\n\n"
        "Please enter the **quantity** you wish to purchase (e.g., 5, 10):",
        parse_mode='Markdown'
    )
    return ASK_QUANTITY

async def process_quantity_and_confirm(update: Update, context):
    try:
        quantity = int(update.message.text.strip())
        if quantity <= 0: raise ValueError
    except ValueError:
        await update.message.reply_text("❌ Invalid quantity. Please enter a valid number greater than zero.")
        return ASK_QUANTITY
        
    product_key = context.user_data.get('selected_product_key')
    details = get_product_details(product_key)
    
    # Reload stock here for a final check before order creation
    load_initial_stock() 
    
    if not details or quantity > PRODUCTS[details['type']][product_key.split('_', 1)[1]]['stock']:
        await update.message.reply_text("❌ The quantity requested is greater than the available stock. Please enter a lower quantity.")
        return ASK_QUANTITY
        
    total_price = quantity * details['price']
    order_id = generate_order_id()
    
    order = {
        'order_id': order_id,
        'product_key': product_key,
        'details': details,
        'quantity': quantity,
        'total_price': total_price,
        'status': 'CONFIRMATION_PENDING',
        'created_at': datetime.now().isoformat()
    }
    context.user_data['current_order'] = order
    
    confirmation_text = (
        f"📝 **ORDER SUMMARY**\n"
        f"--- \n"
        f"**Order ID**: `{order_id}`\n"
        f"**Product**: {details['name']}\n"
        f"**Price/Unit**: ${details['price']:.2f}\n"
        f"**Quantity**: {quantity}\n"
        f"--- \n"
        f"**Total Amount Due**: **${total_price:.2f}**\n\n"
        "Please confirm to proceed to payment instructions."
    )
    
    keyboard = [
        [InlineKeyboardButton("✅ Confirm & Pay", callback_data="ORDER_PAYMENT")],
        [InlineKeyboardButton("❌ Cancel Order", callback_data="ORDER_CANCEL")]
    ]
    
    await update.message.reply_text(
        confirmation_text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )
    return CONFIRM_ORDER

async def make_payment(update: Update, context):
    await update.callback_query.answer()
    order = context.user_data.get('current_order')
    if not order:
        await update.callback_query.edit_message_text("❌ Order session expired. Please start a new order with /start.", parse_mode='Markdown')
        return HOME
        
    # Generate and store unique payment amount
    payment_data = store_payment_data(
        order['order_id'],
        update.effective_user.id,
        order['total_price']
    )
    # This is now the full 6-decimal string (e.g., '0.100001')
    unique_amount_full_str = payment_data['unique_amount']
    
    # Update order in global database
    contract_info = context.user_data.get('contract_info', {})
    order.update({
        'customer_id': update.effective_user.id,
        'buyer_name': contract_info.get('name', 'Unknown'),
        'buyer_country': contract_info.get('country', 'Unknown'),
        'buyer_whatsapp': contract_info.get('contact', 'Unknown'),
        'status': 'PENDING_PAYMENT'
    })
    ORDER_DATABASE[order['order_id']] = order
    
    # Create instructions, which will strip the full string for display
    payment_instructions = create_manual_payment_instructions(
        order['order_id'], order['total_price'], unique_amount_full_str
    )
    
    keyboard = [[InlineKeyboardButton("❌ Cancel Order", callback_data="ORDER_CANCEL")]]
    await update.callback_query.edit_message_text(
        payment_instructions, 
        reply_markup=InlineKeyboardMarkup(keyboard), 
        parse_mode='Markdown'
    )
    return AWAITING_TX_ID

# RESTORED FUNCTION: submit_transaction_id_and_verify
async def submit_transaction_id_and_verify(update: Update, context):
    query = update.callback_query
    await query.answer()
    
    if query.data == "ORDER_TX_RE_ENTER":
        # Handle re-entering TX ID via button click
        await query.edit_message_text(
            "↩️ **Re-enter Transaction ID**\n\nPlease reply to this message with your Transaction ID (TX ID) again:",
            parse_mode='Markdown'
        )
        return AWAITING_TX_ID
        
    # Fallback/Error state for the button. Should not happen if AWAITING_TX_ID is set correctly
    await query.edit_message_text("❌ Submission failed or session expired. Please start a new order with /start.", parse_mode='Markdown')
    return HOME
    
async def submit_transaction_id(update: Update, context):
    tx_id = update.message.text.strip()
    order = context.user_data.get('current_order')
    if not order:
        await update.message.reply_text("❌ Order session expired. Please start a new order with /start.")
        return HOME
    
    order['status'] = 'TX_SUBMITTED'
    order['tx_id'] = tx_id
    
    # Fetch the full 6-decimal unique amount associated with this pending order
    payment_db_cursor.execute('SELECT unique_amount FROM pending_payments WHERE order_id = ?', (order['order_id'],))
    result = payment_db_cursor.fetchone()
    # unique_amount_str is the full 6-decimal string (e.g., '0.100001')
    unique_amount_str = result[0] if result else f"{Decimal(order['total_price']):.6f}"
    order['customer_id'] = update.effective_user.id 
    
    # Check if the unique amount was already received and is waiting for a match
    payment_db_cursor.execute('''
        SELECT unique_amount FROM received_payments WHERE unique_amount = ? AND status = 'unmatched'
    ''', (unique_amount_str,))
    
    received_match = payment_db_cursor.fetchone()
    
    # Display the clean amount for the user/admin notifications
    clean_amount_display = unique_amount_str.rstrip('0').rstrip('.')
    
    if received_match:
        # Instant Match: Payment data came in before the customer submitted the TX ID
        await update.message.reply_text(
            "✅ **TX ID Received.** Payment confirmed instantly! Preparing delivery...",
            parse_mode='Markdown'
        )
        await fulfill_order_process_auto(order['order_id'], order['customer_id'], unique_amount_str, context.bot)
        
        # Notify admin of the instant success
        await context.bot.send_message(
            chat_id=ADMIN_CHAT_ID,
            text=f"🔥 **INSTANT AUTO-MATCH SUCCESS!** 🔥\n"
                 f"Customer submitted TX ID after you submitted the payment data.\n"
                 f"**Order ID**: `{order['order_id']}`\n"
                 f"**Amount Matched**: `{clean_amount_display}`\n\n"
                 f"The order has been automatically fulfilled.",
            parse_mode='Markdown'
        )
    else:
        # Pending Match: Await admin submission of payment data
        # NOTE: Added a button to re-submit TX ID for the customer if they made a mistake
        keyboard = [[InlineKeyboardButton("↩️ Re-enter TX ID", callback_data="ORDER_TX_RE_ENTER")]]
        
        await update.message.reply_text(
            "⏳ **TX ID Received.** Your order is now **PENDING** verification.\n"
            "We are awaiting the admin's payment received data to confirm your unique amount.\n"
            "You will be notified immediately when verification is complete.",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )
        # Notify admin that an order is waiting
        admin_message = (
            "⚠️ **NEW PENDING ORDER (AWAITING YOUR DATA)**\n"
            f"--- \n"
            f"**Order ID**: `{order['order_id']}`\n"
            f"**Customer**: {order.get('buyer_name', 'Unknown')}\n"
            f"**Product**: {order['details']['name']} (x{order['quantity']})\n"
            f"**Expected Unique Amount**: `{clean_amount_display} USD`\n" 
            f"**TX ID Provided**: `{tx_id}`\n"
            f"--- \n"
            "Please send the corresponding '💰 NEW CRYPTO PAYMENT RECEIVED!' message to verify this order."
        )
        await context.bot.send_message(
            chat_id=ADMIN_CHAT_ID,
            text=admin_message,
            parse_mode='Markdown'
        )
    return HOME

async def cancel_order(update: Update, context):
    await update.callback_query.answer()
    order = context.user_data.pop('current_order', None)
    
    # Clean up order and pending payment records
    if order:
        if order['order_id'] in ORDER_DATABASE:
            ORDER_DATABASE.pop(order['order_id'])
        payment_db_cursor.execute('UPDATE pending_payments SET status = ? WHERE order_id = ?', ('cancelled', order['order_id']))
        payment_db_conn.commit()
        
    await update.callback_query.edit_message_text("🗑️ **Order Cancelled.** You can start a new order anytime.", parse_mode='Markdown')
    return HOME

async def start(update: Update, context):
    user = update.effective_user
    
    # Robustly handle the first name for Markdown parsing safety
    safe_name = re.sub(r'([_*[\]()~`>#+\-=|{}.!])', r'\\\1', user.first_name) if user.first_name else "User"

    if str(user.id) == ADMIN_CHAT_ID:
        keyboard = [
            [InlineKeyboardButton("🛠️ Open Admin Panel", callback_data="ADMIN_PANEL_START")],
            [InlineKeyboardButton("🛒 View Shop (Locked)", callback_data="ACTION_LOCKED")]
        ]
        welcome_text = (
            f"👋 Welcome, **Administrator**! Use the Admin Panel to manage prices and verification data. "
            f"The Shop option is locked for you."
        )
    else:
        is_signed_up = context.user_data.get('contract_info', {}).get('name')
        
        keyboard = [
            [InlineKeyboardButton("📝 Sign Up / Contact Info", callback_data="ACTION_SIGNUP")],
            [InlineKeyboardButton("🛒 Buy Products", callback_data="ACTION_BUY")]
        ]
        
        welcome_text = (
            f"👋 Welcome, **{safe_name}**! I'm your automated product delivery bot.\n"
            f"My system uses a **Unique Amount** for secure payment verification.\n"
            f"--- \n" +
            ("⚠️ **Please register your contact information first to proceed with purchases.**" if not is_signed_up else "✅ **You are ready to shop!** Select 'Buy Products' below.")
        )
    
    if update.callback_query:
        await update.callback_query.edit_message_text(
            welcome_text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )
    else:
        await update.effective_message.reply_text(
            welcome_text,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode='Markdown'
        )
    return HOME

async def fallback(update: Update, context):
    # Admin is sending payment data (highest priority)
    if update.message:
        # Save the current state before potentially changing it for Admin/Fallback
        current_state = context.user_data.get('state', HOME)
        
        if str(update.effective_user.id) == ADMIN_CHAT_ID and 'NEW CRYPTO PAYMENT RECEIVED' in update.message.text:
            # Prevent admin from accidentally submitting payment data while in a price setting state
            if current_state == ADMIN_AWAITING_PRICE:
                await update.message.reply_text("⚠️ **Attention:** You are currently in the price setting mode. Please send the price first or type **cancel**.", parse_mode='Markdown')
                return ADMIN_AWAITING_PRICE
                
            return await process_admin_received_data(update, context)

        await update.message.reply_text("I didn't understand that. Please use the menu buttons or type /start to go to the home menu.")
        
    elif update.callback_query:
        if update.callback_query.data == "ACTION_LOCKED":
             await update.callback_query.answer("Admin accounts are restricted from placing orders.")
        else:
            await update.callback_query.answer("Please use the menu buttons or type /start to go to the home menu.")
            
    # Return the current state to keep the conversation going, or HOME if not set
    return context.user_data.get('state', HOME) 


# --- MAIN EXECUTION ---

def main():
    
    # --- Initialization and Stock Loading (Synchronous) ---
    try:
        # Load stock counts from Google Sheets upon startup
        if get_google_sheet_client():
            load_initial_stock()
        else:
            logger.warning("❌ Google Sheets connection failed. Using local/zero stock counts.")
    except Exception as e:
        logger.error(f"Initialization failed (could not load stock): {e}")

    # Create the Telegram Application
    application = Application.builder().token(BOT_TOKEN).build()
    
    # Add the global error handler
    application.add_error_handler(error_handler)
    
    # Conversation Handler defines the main bot logic flow
    conv_handler = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={
            HOME: [
                CallbackQueryHandler(signup_start, pattern="^ACTION_SIGNUP$"), 
                CallbackQueryHandler(buy_start, pattern="^ACTION_BUY$"),      
                CallbackQueryHandler(start, pattern="^ACTION_START$"),        
                CallbackQueryHandler(admin_panel_start, pattern="^ADMIN_PANEL_START$"),
            ],
            SIGNUP_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, signup_name)],
            SIGNUP_COUNTRY: [MessageHandler(filters.TEXT & ~filters.COMMAND, signup_country)],
            SIGNUP_CONTACT: [MessageHandler(filters.TEXT & ~filters.COMMAND, signup_contact)],
            SELECT_PRODUCT_TYPE: [
                CallbackQueryHandler(select_product_list, pattern="^TYPE_(BM|NEXTDOOR)$"),
                CallbackQueryHandler(start, pattern="^ACTION_START$"),
            ],
            SELECT_PRODUCT: [
                CallbackQueryHandler(ask_quantity, pattern="^PRODUCT_|^NO_STOCK$"),
                CallbackQueryHandler(buy_start, pattern="^ACTION_BUY$"), 
            ],
            ASK_QUANTITY: [MessageHandler(filters.TEXT & ~filters.COMMAND, process_quantity_and_confirm)],
            CONFIRM_ORDER: [
                CallbackQueryHandler(cancel_order, pattern="^ORDER_CANCEL$"), 
                CallbackQueryHandler(make_payment, pattern="^ORDER_PAYMENT$"), 
            ],
            # RESTORED STATE LOGIC
            AWAITING_TX_ID: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, submit_transaction_id),
                CallbackQueryHandler(cancel_order, pattern="^ORDER_CANCEL$"), 
                CallbackQueryHandler(submit_transaction_id_and_verify, pattern="^ORDER_TX_RE_ENTER$"), 
            ],
            # ADMIN FLOW
            ADMIN_PANEL: [
                CallbackQueryHandler(set_product_price_prompt, pattern="^ADMIN_SET_PRICE_PROMPT$"),
                CallbackQueryHandler(view_order_statistics, pattern="^ADMIN_VIEW_STATS$"),
                CallbackQueryHandler(view_pending_orders, pattern="^ADMIN_VIEW_PENDING$"),
                CallbackQueryHandler(receive_data_info, pattern="^ADMIN_RECEIVE_DATA_INFO$"),
                CallbackQueryHandler(start, pattern="^ACTION_START$"),
            ],
            ADMIN_AWAITING_PRICE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, process_new_price),
            ],
        },
        fallbacks=[
            CommandHandler("start", start),
            CallbackQueryHandler(start, pattern="^ACTION_START$"),
            MessageHandler(filters.ALL, fallback) 
        ],
    )

    application.add_handler(conv_handler)

    logger.info("Bot is starting with all fixed features and restored logic...")
    
    # Start the bot polling. This is the blocking call that runs the event loop.
    try:
        application.run_polling(poll_interval=3)
    except KeyboardInterrupt:
        # Gracefully close connections on interruption
        payment_db_conn.close()
        logger.info("Bot stopped by user.")
    except Exception as e:
        logger.critical(f"Fatal error during application run: {e}")
        payment_db_conn.close()
        
if __name__ == "__main__":
    main()