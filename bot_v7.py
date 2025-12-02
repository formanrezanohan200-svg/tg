import logging
from logging import StreamHandler
import gspread
import traceback
from datetime import datetime
from uuid import uuid4
import sys

# --- External Libraries Imports ---
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    ConversationHandler,
    filters,
)
from telegram.constants import ParseMode
from telegram.error import TelegramError
from gspread.exceptions import APIError, SpreadsheetNotFound, WorksheetNotFound

# --- 1. CONFIGURATION AND INITIAL SETUP ---

# IMPORTANT: These values are taken from your inputs.
BOT_TOKEN = "8320244797:AAEO29HiJnD_xehOrMJOf-eHB6gexk2PLm0" 
ADMIN_ID = 6718122711 
SPREADSHEET_ID = "180UVbuZb__TynCosXHnGWD48uREmnPwdWDVcUh1Wr0c" 
CREDENTIALS_FILE = "credentials.json" 

# Define Conversation States
(SELECT_PRODUCT, AWAITING_PROOF, CONFIRM_ORDER, ADMIN_MENU, ADMIN_SET_PRICE, ADMIN_SET_STOCK) = range(6)

# Configure Logging (Logs to console and file)
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
    handlers=[
        logging.FileHandler('bot_logs.log', mode='a'),
        StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# --- 2. GOOGLE SHEETS INTEGRATION ---

# Global variables for Sheets access
gc = None
products_sheet = None
orders_sheet = None
users_sheet = None

def init_sheets():
    """Initializes gspread client and loads all required worksheets."""
    global gc, products_sheet, orders_sheet, users_sheet
    try:
        logger.info(f"Attempting to authenticate with '{CREDENTIALS_FILE}'...")
        gc = gspread.service_account(filename=CREDENTIALS_FILE)
        
        logger.info(f"Opening spreadsheet with ID: {SPREADSHEET_ID}")
        spreadsheet = gc.open_by_key(SPREADSHEET_ID)
        
        # Load the specific worksheets (requires exact names: Products, Orders, Users)
        products_sheet = spreadsheet.worksheet("Products")
        orders_sheet = spreadsheet.worksheet("Orders")
        users_sheet = spreadsheet.worksheet("Users")
        
        logger.info("Google Sheets initialized successfully. Bot is ready.")
    except FileNotFoundError:
        logger.critical(f"FATAL: Credentials file '{CREDENTIALS_FILE}' not found. Check file path.")
        raise RuntimeError("Sheets Initialization Failed (Credentials missing)")
    except SpreadsheetNotFound:
        logger.critical(f"FATAL: Spreadsheet ID '{SPREADSHEET_ID}' is invalid or bot user lacks access.")
        raise RuntimeError("Sheets Initialization Failed (Spreadsheet not found)")
    except WorksheetNotFound as e:
        logger.critical(f"FATAL: Required sheet tab is missing. Check names: {e}. Tabs must be named 'Products', 'Orders', and 'Users'.")
        raise RuntimeError("Sheets Initialization Failed (Missing tab)")
    except Exception as e:
        logger.critical(f"FATAL: Google Sheets initialization failed (Check API/Sharing): {e}")
        raise RuntimeError("Sheets Initialization Failed (General Error)")

def get_product_data() -> dict:
    """
    Reads all product data and consolidates by Product Name, summing the stock, 
    to ensure only one entry per unique product is displayed.
    """
    try:
        # Get all records for consolidation logic
        data = products_sheet.get_all_records()
        consolidated_products = {}
        
        for row in data:
            name = row.get('Name')
            sku = row.get('SKU')
            
            if not name or not sku:
                continue

            try:
                # Stock should be 1 for items ready for sale, 0 for items sold
                current_stock = int(row.get('Stock', 0)) 
            except (ValueError, TypeError):
                current_stock = 0
            
            # Use Product Name as the primary consolidation key
            consolidation_key = name.strip().upper()

            if consolidation_key in consolidated_products:
                # Sum the stock from all duplicate rows
                existing_entry = consolidated_products[consolidation_key]
                existing_entry['Stock'] += current_stock
            else:
                # First time seeing this product name
                row['Stock'] = current_stock
                consolidated_products[consolidation_key] = row
                
        # The key for the final dict is the SKU of the first entry found for that product name
        final_products = {p['SKU']: p for p in consolidated_products.values()}
            
        return final_products
    except Exception as e:
        logger.error(f"Error reading product data: {e}")
        return {}

def process_delivery_and_update_stock(product_name: str, quantity: int) -> tuple[str, bool]:
    """
    Finds the exact number of rows (equal to quantity) matching the product name, 
    extracts the Delivery Content, and marks them as sold (Stock=0, Content cleared).
    Returns the concatenated delivery content and success status.
    
    NOTE: This assumes each row with Stock=1 and Delivery Content represents 1 unit.
    """
    try:
        # Get all sheet data to process rows, including header row
        all_data = products_sheet.get_all_values()
        header = all_data[0]
        data_rows = all_data[1:] 
        
        # Determine column indices from header
        name_col_index = header.index('Name')
        stock_col_index = header.index('Stock')
        delivery_col_index = header.index('Delivery Content')
        
        target_name = product_name.strip()
        delivery_content_list = []
        rows_to_update = []
        
        # 1. Find available units
        for row_index, row in enumerate(data_rows):
            # Check if we have enough items
            if len(delivery_content_list) >= quantity:
                break

            try:
                current_stock = int(row[stock_col_index])
            except (ValueError, IndexError):
                current_stock = 0 # Treat non-numeric/missing stock as 0
            
            # Match Name, check if Stock is 1 (available), and check if Delivery Content exists
            if row[name_col_index].strip() == target_name and \
               current_stock >= 1 and \
               row[delivery_col_index].strip():
                
                delivery_content_list.append(row[delivery_col_index].strip())
                # Store row number (1-indexed for gspread)
                rows_to_update.append(row_index + 2) 

        # 2. Check availability
        if len(delivery_content_list) < quantity:
            logger.warning(f"Insufficient stock found for delivery of {quantity} units of {product_name}. Found: {len(delivery_content_list)}.")
            return "⚠️ Insufficient stock available in the sheet for delivery.", False

        # 3. Perform Stock Update / Mark as Delivered
        update_range = []
        for row_num in rows_to_update:
            # Mark Stock as 0 (Column D / index 4)
            update_range.append({'range': f'D{row_num}', 'values': [[0]]}) 
            # Clear Delivery Content (Column E / index 5)
            update_range.append({'range': f'E{row_num}', 'values': [['DELIVERED']]}) # Use "DELIVERED" placeholder

        # Perform the batch update
        if update_range:
            products_sheet.batch_update(update_range)
        
        # Concatenate content for delivery (separated by newlines)
        final_content = "\n\n---\n\n".join(delivery_content_list)
        return final_content, True
        
    except Exception as e:
        logger.error(f"Error during bulk delivery and stock update for {product_name}: {e}")
        return f"⚠️ Critical Error during fulfillment: {e}", False


def log_order(order_data: dict) -> None:
    """Logs a new order to the 'Orders' sheet."""
    try:
        # Assuming the Orders sheet has these columns:
        # A: OrderID, B: Timestamp, C: UserID, D: Username, E: SKU, F: Price (Total), G: Quantity, H: Status, I: ProofID
        row = [
            order_data.get('OrderID'),       # A
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"), # B
            order_data.get('UserID'),        # C
            order_data.get('Username'),      # D
            order_data.get('SKU'),           # E
            order_data.get('Price'),         # F 
            order_data.get('Quantity'),      # G 
            order_data.get('Status', 'Pending'), # H
            order_data.get('ProofID', '')    # I
        ]
        orders_sheet.append_row(row)
        logger.info(f"Order {order_data['OrderID']} logged in Orders sheet with quantity {order_data['Quantity']}.")
    except Exception as e:
        logger.error(f"Error logging order {order_data.get('OrderID')}: {e}")

# --- 3. USER HANDLERS (E-COMMERCE FLOW) ---

async def go_to_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Helper function to reset the state and send the start message."""
    if update.callback_query:
        await update.callback_query.answer()
        
    user = update.effective_user
    keyboard = [
        [InlineKeyboardButton("🛒 Browse Products", callback_data='shop')],
        [InlineKeyboardButton("📜 My Order History", callback_data='history')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    # Use edit_message_text if coming from a callback, otherwise send a new message
    if update.callback_query:
        await update.callback_query.edit_message_text(
            f"👋 Welcome back, **{user.first_name}**!\n\n"
            "Use the menu below to browse our verified digital products.",
            reply_markup=reply_markup,
            parse_mode=ParseMode.MARKDOWN
        )
    elif update.message:
        await update.message.reply_text(
            f"👋 Welcome back, **{user.first_name}**!\n\n"
            "Use the menu below to browse our verified digital products.",
            reply_markup=reply_markup,
            parse_mode=ParseMode.MARKDOWN
        )
        
    return SELECT_PRODUCT 

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Sends the welcome message and main menu."""
    # Simple user tracking
    try:
        user = update.effective_user
        # Log user details
        users_sheet.append_row([user.id, user.username, user.full_name, datetime.now().strftime("%Y-%m-%d %H:%M:%S")])
    except Exception as e:
        logger.warning(f"Could not log user {user.id}: {e}")
        
    return await go_to_main_menu(update, context)

async def show_products(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Displays the list of consolidated products using inline buttons."""
    query = update.callback_query
    if query:
        await query.answer()

    products = get_product_data()
    if not products:
        text = "⚠️ **Error:** Could not load products. Please try again later." # Handling error from image_edad7e.png
        if query:
            await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN)
        elif update.message:
            await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)
        return SELECT_PRODUCT

    keyboard = []
    text = "🔥 **Our Products:**\n\n"

    for sku, data in products.items():
        try:
            # Note: data['Stock'] here contains the *SUMMED* stock (from get_product_data)
            stock = int(data.get('Stock', 0)) 
        except (ValueError, TypeError):
            stock = 0
            
        name = data.get('Name', 'N/A')
        price = data.get('Price (USD)', 'N/A')

        stock_display = f"Stock: {stock}"
        
        # Display the product list (now only once per unique name)
        text += f"*{name}* - **${price}** ({stock_display})\n"
        
        # Add only ONE button per unique SKU/Product
        if stock > 0:
            keyboard.append([InlineKeyboardButton(f"Buy {name} (${price})", callback_data=f'qty_prompt_{sku}')])
        else:
            keyboard.append([InlineKeyboardButton(f"Buy {name} (Sold Out)", callback_data='ignore')])

    keyboard.append([InlineKeyboardButton("⬅️ Main Menu", callback_data='main_menu')])
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    if query:
        await query.edit_message_text(text, reply_markup=reply_markup, parse_mode=ParseMode.MARKDOWN)
    elif update.message:
        await update.message.reply_text(text, reply_markup=reply_markup, parse_mode=ParseMode.MARKDOWN)
        
    return SELECT_PRODUCT

async def quantity_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Prompts the user to enter quantity or select from quick options."""
    query = update.callback_query
    await query.answer()
    
    sku = query.data.split('_')[2]
    products = get_product_data()
    product_data = products.get(sku)
    
    if not product_data:
        await query.edit_message_text("🚫 **Error:** Product data is missing. Please try again.")
        return await show_products(update, context)
        
    try:
        stock = int(product_data.get('Stock', 0))
    except (ValueError, TypeError):
        stock = 0
        
    if stock <= 0:
        await query.edit_message_text("🚫 **Error:** Product is sold out or no longer available.")
        return await show_products(update, context)

    context.user_data['selected_sku'] = sku
    context.user_data['product_name'] = product_data['Name']

    quick_options = []
    max_options = min(5, stock)
    for i in range(1, max_options + 1):
        quick_options.append(InlineKeyboardButton(str(i), callback_data=f'buy_qty_{i}'))

    if stock > 5 and stock not in [int(b.text) for b in quick_options]:
        quick_options.append(InlineKeyboardButton(f"Max ({stock})", callback_data=f'buy_qty_{stock}'))

    keyboard = [quick_options, [InlineKeyboardButton("⬅️ Back to Products", callback_data='shop')]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    text = (
        f"🛒 How many *{product_data['Name']}* (Max: {stock}) would you like to buy?\n\n"
        "Select a quantity below or **type in the exact number**."
    )
    
    await query.edit_message_text(text, reply_markup=reply_markup, parse_mode=ParseMode.MARKDOWN)
    
    return CONFIRM_ORDER 

async def quantity_input_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handles text input for quantity or callback for quick quantity selection."""
    
    # 1. Handle Callback Query (Quick Select)
    if update.callback_query:
        query = update.callback_query
        await query.answer()
        
        if query.data.startswith('buy_qty_'):
            try:
                quantity = int(query.data.split('_')[2])
            except ValueError:
                await query.edit_message_text("⚠️ Invalid selection. Please try again.")
                return await quantity_prompt(update, context)
            
            # Go to checkout
            return await checkout(update, context, quantity=quantity)
        
        elif query.data == 'shop':
            return await show_products(update, context)

    # 2. Handle Text Input (Manual Quantity)
    elif update.message and update.message.text:
        try:
            quantity = int(update.message.text)
        except ValueError:
            await update.message.reply_text("⚠️ Please enter a valid whole number for the quantity.")
            return CONFIRM_ORDER
            
        # Go to checkout
        if not context.user_data.get('selected_sku'):
            await update.message.reply_text("⚠️ Error: Please select a product first.")
            return await show_products(update, context)
            
        return await checkout(update, context, quantity=quantity)

    await update.message.reply_text("Please select a quantity or type a number.")
    return CONFIRM_ORDER


async def checkout(update: Update, context: ContextTypes.DEFAULT_TYPE, quantity: int = 1) -> int:
    """Handles product selection and initiates the manual payment process."""
    
    sku = context.user_data.get('selected_sku')
    
    effective_update = update.callback_query if update.callback_query else update.message

    if not sku:
        if effective_update:
            await effective_update.reply_text("⚠️ **Error:** Order session lost. Please start over.")
        return await go_to_main_menu(update, context)

    products = get_product_data()
    product_data = products.get(sku)
    
    if not product_data:
        if effective_update:
            await effective_update.reply_text("⚠️ **Error:** Product data missing. Please try again.")
        return await go_to_main_menu(update, context)

    try:
        current_stock = int(product_data.get('Stock', 0))
    except (ValueError, TypeError):
        current_stock = 0

    if current_stock < quantity or quantity <= 0:
        if effective_update:
            await effective_update.reply_text(
                f"🚫 **Error:** Requested quantity ({quantity}) is not available (Stock: {current_stock})."
            )
        return await show_products(update, context)
    
    # --- Create Order ---
    order_id = str(uuid4())[:8].upper()
    user = update.effective_user
    
    # Calculate Total Price
    unit_price = float(product_data.get('Price (USD)', 0))
    total_price = unit_price * quantity
    
    # Store the Name that was displayed to the user
    product_name = product_data['Name'] 
    
    order_data = {
        'OrderID': order_id,
        'UserID': user.id,
        'Username': user.username or user.full_name,
        'SKU': sku, 
        'Price': total_price, 
        'Quantity': quantity,
        'Name': product_name, # Store product name for delivery retrieval
        'Status': 'Pending'
    }
    
    context.user_data['current_order'] = order_data
    log_order(order_data)

    # --- Payment Instruction ---
    payment_address = "777904898"
    
    instruction = (
        f"🧾 **Order Summary**\n"
        f"Product: *{product_name}* (x{quantity})\n"
        f"Unit Price: ${unit_price:.2f}\n"
        f"Total Price: **${total_price:.2f}**\n"
        f"Unique Order ID: `{order_id}`\n\n"
        
        "**To Complete Your Purchase (Manual/Crypto Payment):**\n"
        f"1. Send **${total_price:.2f}** to the following crypto address:\n"
        f"   Address: `{payment_address}`\n" 
        f"   Amount: `${total_price:.2f}` USD\n" 
        "2. **IMPORTANT:** Save your Transaction ID.\n"
        "3. Once sent, click the **'I Have Paid'** button below and send us the **Transaction ID or proof screenshot**."
    )
    
    keyboard = [
        [InlineKeyboardButton("✅ I Have Paid (Send Proof)", callback_data='paid_proof')],
        [InlineKeyboardButton("❌ Cancel Order", callback_data='cancel_order')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    if update.callback_query:
        await update.callback_query.edit_message_text(
            instruction, reply_markup=reply_markup, parse_mode=ParseMode.MARKDOWN
        )
    elif update.message:
        await update.message.reply_text(
            instruction, reply_markup=reply_markup, parse_mode=ParseMode.MARKDOWN
        )
    
    return AWAITING_PROOF

async def cancel_order(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Cancels the current order and returns to the main menu."""
    query = update.callback_query
    await query.answer()

    order_data = context.user_data.get('current_order')
    if order_data:
        context.user_data.pop('current_order')
        
        await query.edit_message_text(
            f"❌ Order `{order_data.get('OrderID', 'N/A')}` has been canceled. Returning to the main menu."
        )
    else:
        await query.edit_message_text("❌ No active order to cancel. Returning to the main menu.")

    return await go_to_main_menu(update, context)

async def start_proof_submission(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Sets the state to receive payment proof."""
    query = update.callback_query
    await query.answer()

    if not context.user_data.get('current_order'):
        await query.edit_message_text("⚠️ **Error:** Your order session has expired. Please start over.")
        return await go_to_main_menu(update, context)

    keyboard = [
        [InlineKeyboardButton("⬅️ Back to Payment Details", callback_data='back_to_checkout')],
        [InlineKeyboardButton("❌ Cancel Order", callback_data='cancel_order')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await query.edit_message_text(
        "📸 **Awaiting Proof:**\n\n"
        "Please reply to this message with your **Transaction ID** as text or send a **screenshot** as a photo.",
        reply_markup=reply_markup,
        parse_mode=ParseMode.MARKDOWN
    )
    
    return AWAITING_PROOF

async def back_to_checkout(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Uses the existing order data to regenerate the checkout message."""
    query = update.callback_query
    await query.answer()
    
    order_data = context.user_data.get('current_order')
    if not order_data:
        await query.edit_message_text("⚠️ **Error:** Order session lost. Returning to main menu.")
        return await go_to_main_menu(update, context)

    total_price = order_data['Price']
    quantity = order_data['Quantity']
    sku = order_data['SKU']
    order_id = order_data['OrderID']
    product_name = order_data['Name']
    
    products = get_product_data()
    product_data = products.get(sku)
    unit_price = float(product_data.get('Price (USD)', total_price / quantity if quantity else 0)) 
    
    payment_address = "777904898"

    instruction = (
        f"🧾 **Order Summary**\n"
        f"Product: *{product_name}* (x{quantity})\n"
        f"Unit Price: ${unit_price:.2f}\n"
        f"Total Price: **${total_price:.2f}**\n"
        f"Unique Order ID: `{order_id}`\n\n"
        
        "**To Complete Your Purchase (Manual/Crypto Payment):**\n"
        f"1. Send **${total_price:.2f}** to the following crypto address:\n"
        f"   Address: `{payment_address}`\n"
        f"   Amount: `${total_price:.2f}` USD\n"
        "2. **IMPORTANT:** Save your Transaction ID.\n"
        "3. Once sent, click the **'I Have Paid'** button below and send us the **Transaction ID or proof screenshot**."
    )
    
    keyboard = [
        [InlineKeyboardButton("✅ I Have Paid (Send Proof)", callback_data='paid_proof')],
        [InlineKeyboardButton("❌ Cancel Order", callback_data='cancel_order')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await query.edit_message_text(instruction, reply_markup=reply_markup, parse_mode=ParseMode.MARKDOWN)
    
    return AWAITING_PROOF

async def receive_proof(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """
    Receives the payment proof (text or photo) and notifies the admin.
    """
    order_data = context.user_data.get('current_order')
    if not order_data:
        await update.message.reply_text("⚠️ **Error:** Your order session has expired. Please start over.")
        return await go_to_main_menu(update, context)

    user = update.effective_user
    
    proof_content = ""
    proof_file_id = None
    
    if update.message.photo:
        proof_file_id = update.message.photo[-1].file_id
        proof_content = f"Photo Proof (File ID: {proof_file_id})"
    elif update.message.text:
        proof_content = f"Text Proof (TXN/ID): <pre>{update.message.text}</pre>"
    else:
        await update.message.reply_text("Please send a **Transaction ID** as text or a **screenshot** as a photo.", parse_mode=ParseMode.MARKDOWN)
        return AWAITING_PROOF

    notification = (
        f"🔔 <b>NEW PENDING PAYMENT</b> 🔔\n\n"
        f"<b>Order ID:</b> <code>{order_data['OrderID']}</code>\n"
        f"<b>Product:</b> {order_data['Name']} (x{order_data['Quantity']}) for ${order_data['Price']:.2f}\n"
        f"<b>Buyer:</b> @{user.username} (ID: {user.id})\n"
        f"<b>Proof:</b> {proof_content}"
    )
    
    keyboard = [
        [InlineKeyboardButton("✅ Verify & Deliver", callback_data=f"verify_{order_data['OrderID']}_paid")],
        [InlineKeyboardButton("❌ Decline", callback_data=f"verify_{order_data['OrderID']}_failed")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    try:
        if proof_file_id:
            await context.bot.send_photo(
                ADMIN_ID, 
                proof_file_id, 
                caption=notification, 
                reply_markup=reply_markup, 
                parse_mode=ParseMode.HTML
            )
        else:
            await context.bot.send_message(
                ADMIN_ID, 
                notification, 
                reply_markup=reply_markup, 
                parse_mode=ParseMode.HTML
            )
    except Exception as e:
        logger.error(f"Failed to send admin notification (HTML failure): {e}")


    keyboard_user = [[InlineKeyboardButton("🏠 Go to Main Menu", callback_data='main_menu')]]
    reply_markup_user = InlineKeyboardMarkup(keyboard_user)

    await update.message.reply_text(
        "✅ **Proof Submitted!**\n\n"
        "We have received your payment proof and notified the admin. "
        "Delivery will be made shortly after verification.",
        reply_markup=reply_markup_user,
        parse_mode=ParseMode.MARKDOWN
    )
    
    context.user_data.pop('current_order')
    return SELECT_PRODUCT 

# --- 4. ADMIN HANDLERS (PROTECTED) ---

def admin_only(func):
    """Decorator to ensure only the admin can run the command."""
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        user_id = update.effective_user.id if update.effective_user else None
        
        if user_id != ADMIN_ID:
            logger.warning(f"Unauthorized admin attempt by user {user_id}")
            if update.message:
                await update.message.reply_text("🚫 **Access Denied:** You are not the administrator.")
            elif update.callback_query:
                await update.callback_query.answer("🚫 Access Denied", show_alert=True)
            return ConversationHandler.END
        return await func(update, context, *args, **kwargs)
    return wrapper

@admin_only
async def admin_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Sends the admin dashboard and menu."""
    
    products = get_product_data()
    low_stock_count = sum(1 for p in products.values() if int(p.get('Stock', 0)) < 10)
    
    text = (
        "👑 **ADMIN DASHBOARD** 👑\n\n"
        f"**Low Stock Alert:** {low_stock_count} items (< 10 left)\n"
        f"Use /pending command or button to check new orders."
    )
    
    keyboard = [
        [InlineKeyboardButton("Pending Orders", callback_data='admin_pending')],
        [InlineKeyboardButton("Manage Price", callback_data='admin_set_price')],
        [InlineKeyboardButton("Manage Stock", callback_data='admin_set_stock')],
        [InlineKeyboardButton("Broadcast Message", callback_data='admin_broadcast')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(text, reply_markup=reply_markup, parse_mode=ParseMode.MARKDOWN)
    return ADMIN_MENU

@admin_only
async def list_pending_orders(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Lists pending orders for manual verification."""
    query = update.callback_query
    if query:
        await query.answer()
    
    try:
        all_orders = orders_sheet.get_all_records()
        pending_orders = [o for o in all_orders if o.get('Status') == 'Pending']
    except Exception as e:
        logger.error(f"Error fetching pending orders: {e}")
        await (query.edit_message_text if query else update.message.reply_text)("⚠️ Error fetching orders from sheet.")
        return

    if not pending_orders:
        text = "🎉 **No pending orders found!**"
        await (query.edit_message_text if query else update.message.reply_text)(text, parse_mode=ParseMode.MARKDOWN)
        return

    for order in pending_orders:
        text = (
            f"--- **PENDING ORDER** ---\n"
            f"**Order ID:** `{order['OrderID']}`\n"
            f"**Product:** {order.get('SKU', 'N/A')} (x{order.get('Quantity', 1)}) for ${order.get('Price', 0)}\n"
            f"**User ID:** {order.get('UserID', 'N/A')}\n"
            f"**Time:** {order.get('Timestamp', 'N/A')}\n"
            f"**Proof ID:** {order.get('ProofID', 'N/A')}"
        )
        keyboard = [
            [InlineKeyboardButton("✅ PAID & DELIVER", callback_data=f"verify_{order['OrderID']}_paid")],
            [InlineKeyboardButton("❌ FAILED/CANCEL", callback_data=f"verify_{order['OrderID']}_failed")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await context.bot.send_message(ADMIN_ID, text, reply_markup=reply_markup, parse_mode=ParseMode.MARKDOWN)

    if query:
        await query.delete_message()


async def verify_and_deliver(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Admin callback to confirm payment, fulfill the order, and update stock."""
    query = update.callback_query
    await query.answer()
    
    _, order_id, status = query.data.split('_')
    
    try:
        cell = orders_sheet.find(order_id, in_column=1)
        row_num = cell.row
        order_row = orders_sheet.row_values(row_num)
        
        # A: OrderID, B: Timestamp, C: UserID, D: Username, E: SKU, F: Price, G: Quantity, H: Status, I: ProofID
        order_data = {
            'OrderID': order_row[0],
            'Timestamp': order_row[1],
            'UserID': int(order_row[2]),
            'Username': order_row[3],
            'SKU': order_row[4],
            'Price': float(order_row[5]),
            'Quantity': int(order_row[6]), # Column G (index 6) for Quantity
            'Status': order_row[7] 
        }
        
    except Exception as e:
        logger.error(f"Error finding or parsing order {order_id} for verification: {e}")
        await query.edit_message_text(f"⚠️ **Error:** Could not find or parse order {order_id} in the sheet.")
        return

    # Update the status in the Orders sheet (Status is column H / index 7)
    orders_sheet.update_cell(row_num, 8, status.capitalize())

    user_id = order_data['UserID']
    product_sku = order_data['SKU']
    quantity = order_data['Quantity'] 
    
    # Get the product name associated with the original SKU from the initial product list.
    products_list = get_product_data()
    product_info = products_list.get(product_sku)
    product_name = product_info.get('Name') if product_info else order_data['SKU'] # Fallback to SKU

    if status == 'paid':
        
        # --- CRITICAL FIX: Bulk Delivery and Stock Update ---
        # This function now handles: 
        # 1. Finding enough available items (quantity).
        # 2. Extracting content for all items.
        # 3. Marking those specific rows as sold (Stock=0, Content=DELIVERED).
        delivery_content, success = process_delivery_and_update_stock(product_name, quantity)

        if success:
            # A. Deliver to user (Deliver the full concatenated content)
            try:
                await context.bot.send_message(user_id, 
                    f"🎉 **Order #{order_id} Verified and Delivered!** 🎉\n\n"
                    f"Your *{product_name}* **(x{quantity})** details are:\n\n"
                    f"```\n{delivery_content}\n```\n\n"
                    f"Thank you for your purchase!",
                    parse_mode=ParseMode.MARKDOWN
                )
                admin_msg = f"✅ **SUCCESS!** Order {order_id} verified, delivered to user {user_id}. **{quantity} units** of *{product_name}* were marked as delivered/sold in the Products sheet."
            except TelegramError as te:
                admin_msg = f"⚠️ **Delivery Failed:** User {user_id} blocked bot. Status updated. **{quantity} units** of *{product_name}* were marked as delivered/sold. Delivery must be done manually."
                logger.warning(f"Delivery failed for user {user_id}: {te}")
        else:
            # Delivery failed (e.g., insufficient stock found in process_delivery_and_update_stock)
            admin_msg = f"❌ **FAILURE:** Order {order_id} verified, but automated delivery and stock update failed: {delivery_content}"
            # Revert status in the Orders sheet? For safety, leave as "Paid" and notify admin for manual intervention.
            orders_sheet.update_cell(row_num, 8, "Paid - Manual Fail")

    elif status == 'failed':
        admin_msg = f"❌ **FAILED:** Order {order_id} marked as Failed."
        try:
            await context.bot.send_message(user_id, 
                f"❌ **Payment Failed/Declined**\n\n"
                f"Your payment for Order #{order_id} could not be verified. Please contact the admin for assistance.",
                parse_mode=ParseMode.MARKDOWN
            )
        except TelegramError:
            pass
            
    await query.edit_message_text(admin_msg, parse_mode=ParseMode.MARKDOWN)

# --- 5. ERROR AND DEBUG SYSTEM ---

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Log the error and send a traceback to the admin."""
    logger.error("Exception while handling an update:", exc_info=context.error)

    tb_list = traceback.format_exception(None, context.error, context.error.__traceback__)
    tb_string = "".join(tb_list)
    
    message = (
        "🚨 **BOT CRASH ALERT** 🚨\n\n"
        f"**Error:** `{context.error}`\n\n"
        f"**Traceback (Truncated):**\n```python\n{tb_string[:1500]}...```"
    )
    
    try:
        await context.bot.send_message(
            chat_id=ADMIN_ID,
            text=message,
            parse_mode=ParseMode.MARKDOWN
        )
    except Exception as e:
        logger.critical(f"Failed to send error notification to Admin: {e}")


# --- 6. MAIN FUNCTION ---

def main() -> None:
    """Start the bot."""
    try:
        init_sheets()
    except RuntimeError:
        logger.critical("Bot failed to initialize Google Sheets. Exiting application.")
        return

    application = Application.builder().token(BOT_TOKEN).build()

    # --- Conversation Handler for E-Commerce Flow ---
    ecom_handler = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={
            SELECT_PRODUCT: [
                CallbackQueryHandler(show_products, pattern='^shop$'),
                CallbackQueryHandler(go_to_main_menu, pattern='^main_menu$'),
                CallbackQueryHandler(quantity_prompt, pattern='^qty_prompt_'),
                CommandHandler("shop", show_products),
            ],
            CONFIRM_ORDER: [
                CallbackQueryHandler(quantity_input_handler, pattern='^buy_qty_|^shop$'),
                MessageHandler(filters.TEXT & ~filters.COMMAND, quantity_input_handler),
            ],
            AWAITING_PROOF: [
                CallbackQueryHandler(start_proof_submission, pattern='^paid_proof$'),
                CallbackQueryHandler(cancel_order, pattern='^cancel_order$'),
                CallbackQueryHandler(back_to_checkout, pattern='^back_to_checkout$'),
                MessageHandler(filters.TEXT | filters.PHOTO, receive_proof),
            ],
        },
        fallbacks=[CommandHandler("start", start)],
        per_user=True,
        allow_reentry=True
    )
    
    application.add_handler(ecom_handler)
    application.add_handler(CallbackQueryHandler(go_to_main_menu, pattern='^main_menu$'))

    # --- Admin Handlers (Separate) ---
    application.add_handler(CommandHandler("admin", admin_menu))
    application.add_handler(CallbackQueryHandler(list_pending_orders, pattern='^admin_pending$'))
    application.add_handler(CallbackQueryHandler(verify_and_deliver, pattern='^verify_'))
    
    # --- Register Global Error Handler ---
    application.add_error_handler(error_handler)
    
    logger.info("Bot started successfully and polling...")
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
