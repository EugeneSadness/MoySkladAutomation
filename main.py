from services.google_sheets_handler import get_product_codes_from_sheet, update_product_details_in_sheet, \
    get_products_with_details, update_daily_stats_in_sheet, get_product_codes_from_sheet2, \
    get_products_with_details_sheet2, update_product_details_in_sheet2, update_daily_stats_in_sheet2
from services.moysklad_api import fetch_product_details_by_codes, fetch_customer_orders_for_products
from utils.date_handler import get_current_day_date_range
from auth.moysklad_auth import get_access_token
from auth.google_auth import authenticate_google_sheets

def process_sheet2(client, spreadsheet, token):
    """Handles processing for Sheet2"""
    try:
        worksheet = spreadsheet.worksheet("Лист2")
        
        # Get existing products from Sheet2
        existing_products = get_products_with_details_sheet2(worksheet)
        print(f"Found {len(existing_products)} products with existing details in Sheet2")
        
        # Get all product codes from Sheet2
        product_codes = get_product_codes_from_sheet2(worksheet)
        print(f"Found {len(product_codes)} product codes in Sheet2")
        
        # Get data for new products
        products = fetch_product_details_by_codes(token, product_codes, existing_products)
        
        # Update new products
        update_product_details_in_sheet2(worksheet, products)
        print("New product details updated in Sheet2")
        
        # Get current orders and stock data
        start_date, end_date = get_current_day_date_range()
        orders_data = fetch_customer_orders_for_products(token, start_date, end_date, products)
        print(f"Processed orders for {len(orders_data)} products")
        
        # Update statistics
        update_daily_stats_in_sheet2(worksheet, orders_data)
        print("Daily statistics updated in Sheet2")
        
    except Exception as e:
        print(f"Error processing Sheet2: {e}")

def main():
    CREDENTIALS_PATH = "./cred.json"
    SHEET_NAME = "test"
    MS_USERNAME = "admin@jinux860"
    MS_PASSWORD = "king5681"
    
    try:
        client = authenticate_google_sheets(CREDENTIALS_PATH)
        spreadsheet = client.open(SHEET_NAME)
        
        # Get MoySklad token
        token = get_access_token(MS_USERNAME, MS_PASSWORD)
        
        # Process Sheet1 (original functionality)
        worksheet = spreadsheet.sheet1
        existing_products = get_products_with_details(worksheet)
        print(f"Found {len(existing_products)} products with existing details in Sheet1")
        
        product_codes = get_product_codes_from_sheet(worksheet)
        print(f"Found {len(product_codes)} product codes in Sheet1")
        
        products = fetch_product_details_by_codes(token, product_codes, existing_products)
        update_product_details_in_sheet(worksheet, products)
        print("New product details updated in Sheet1")
        
        start_date, end_date = get_current_day_date_range()
        orders_data = fetch_customer_orders_for_products(token, start_date, end_date, products)
        print(f"Processed orders for {len(orders_data)} products")
        
        update_daily_stats_in_sheet(worksheet, orders_data)
        print("Daily statistics updated in Sheet1")
        
        # Process Sheet2
        process_sheet2(client, spreadsheet, token)
        
        return orders_data
    
    except Exception as e:
        print(f"Error occurred: {e}")
        return None

if __name__ == "__main__":
    main() 