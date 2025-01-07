import time
from datetime import datetime

import pytz
import schedule

from services.google_sheets_handler import (
    get_product_codes_from_sheet, update_product_details_in_sheet,
    get_products_with_details, update_daily_stats_in_sheet,
    get_product_codes_from_sheet2, get_products_with_details_sheet2,
    update_product_details_in_sheet2, update_daily_stats_in_sheet2,
    update_sheet3, get_supply_dates_from_sheet3, update_supply_quantities_in_sheet3,
    get_sales_channels_and_statuses, update_sales_report_in_sheet5, update_categories_costs_in_sheet5,
    update_transits_costs_in_sheet5, update_daily_stats_in_sheet5_sliding_window, sheet3_sliding_window
)
from services.moysklad_api import (
    fetch_product_details_by_codes, fetch_customer_orders_for_products,
    fetch_supplies_by_date_range, fetch_orders_by_channels, fetch_categories_costs, fetch_stock_CHINA_in_transit,
    fetch_url_stock_CHINA_in_transit, fetch_product_stock
)
from utils.date_handler import get_current_day_date_range
import gspread

from auth.google_auth import authenticate_google_sheets
from auth.moysklad_auth import get_access_token
import config

def process_sheet1(spreadsheet, token):
    """Handles processing for Sheet1"""
    worksheet1 = spreadsheet.sheet1
    existing_products = get_products_with_details(worksheet1)
    print(f"Found {len(existing_products)} products with existing details in Sheet1")

    product_codes = get_product_codes_from_sheet(worksheet1)
    print(f"Found {len(product_codes)} product codes in Sheet1")

    products = fetch_product_details_by_codes(token, product_codes, existing_products)
    update_product_details_in_sheet(worksheet1, products)
    print("New product details updated in Sheet1")

    start_date, end_date = get_current_day_date_range()
    orders_data = fetch_customer_orders_for_products(token, start_date, end_date, products)
    print(f"Processed orders for {len(orders_data)} products")

    update_daily_stats_in_sheet(worksheet1, orders_data)
    print("Daily statistics updated in Sheet1")

def process_sheet2(spreadsheet, token):
    """Handles processing for Sheet2"""
    try:
        worksheet = spreadsheet.worksheet("Лист2")
        existing_products = get_products_with_details_sheet2(worksheet)
        print(f"Found {len(existing_products)} products with existing details in Sheet2")
        product_codes = get_product_codes_from_sheet2(worksheet)
        print(f"Found {len(product_codes)} product codes in Sheet2")
        products = fetch_product_details_by_codes(token, product_codes, existing_products)
        update_product_details_in_sheet2(worksheet, products)
        print("New product details updated in Sheet2")
    except Exception as e:
        print(f"Error processing Sheet2: {e}")



def process_sheet3(spreadsheet, token):
    """Обрабатывает данные приемок для Листа3 для будущих дат"""
    try:
        worksheet3 = spreadsheet.worksheet("Лист3")
        #sheet3_sliding_window(worksheet3)
        supply_dates = get_supply_dates_from_sheet3(worksheet3)
        if not supply_dates:
            print("Нет данных для обработки будущих приемок")
            return
        current_date = datetime.now().strftime("%Y-%m-%d")

        # Fetch supplies
        supplies = fetch_supplies_by_date_range(token, current_date)
        supplies_quantities = {}
        for supply in supplies:
            try:
                moment = supply['moment'].split('.')[0]
                supply_date = datetime.strptime(moment, "%Y-%m-%d %H:%M:%S").strftime("%d.%m.%Y")
                for position in supply['positions']:
                    code = position['code']
                    if code in supply_dates and supply_date in supply_dates[code]:
                        if code not in supplies_quantities:
                            supplies_quantities[code] = {}
                        if supply_date not in supplies_quantities[code]:
                            supplies_quantities[code][supply_date] = 0
                        supplies_quantities[code][supply_date] += position['quantity']
            except ValueError as e:
                print(f"Ошибка обработки даты для приемки {supply.get('id')}: {e}")
                continue

        # Fetch product stock quantities
        product_codes = [row[0] for row in worksheet3.get_all_values()[3:] if row[0].strip()]
        stock_quantities = fetch_product_stock(token, product_codes)

        # Update stock quantities in column D
        cells_to_update = []
        for idx, code in enumerate(product_codes, start=4):
            stock_quantity = stock_quantities.get(code, 0)
            cells_to_update.append(gspread.Cell(idx, 4, stock_quantity))  # Column D

        if cells_to_update:
            worksheet3.update_cells(cells_to_update)
            print("Stock quantities updated in column D")

        if supplies_quantities:
            update_supply_quantities_in_sheet3(worksheet3, supplies_quantities)
            print(f"Данные о будущих прием��ах обновлены в Лист3")
        else:
            print("Нет данных о будущих приемках")
    except Exception as e:
        print(f"Ошибка при обработке Лист3: {e}")
        raise

def process_sheet5(worksheet, token):
    """Обрабатывает Лист5: обновляет статистику по заказам и остаткам по категориям"""
    try:
        print("Обрабатывается Лист5")
        #update_daily_stats_in_sheet5_sliding_window(worksheet)
        status_channels = get_sales_channels_and_statuses(worksheet)
        orders_report = fetch_orders_by_channels(token, status_channels)
        categories_costs = fetch_categories_costs(token)
        current_date = datetime.now().strftime("%d.%m.%Y")
        update_sales_report_in_sheet5(worksheet, orders_report, current_date)
        update_categories_costs_in_sheet5(worksheet, categories_costs)
        transits_costs = fetch_stock_CHINA_in_transit(token)
        update_transits_costs_in_sheet5(worksheet, transits_costs)
        print("Лиcт5 успешно обновлен")
    except Exception as e:
        print(f"Ошибка при обработке Лист5: {str(e)}")
        raise 


def schedule_process_sheets(spreadsheet, token):
    moscow_tz = pytz.timezone('Europe/Moscow')

    def update_sheet3_products():
        """Вспомогательная функция для обновления Листа3"""
        worksheet3 = spreadsheet.worksheet("Лист3")
        product_codes = [row[0] for row in worksheet3.get_all_values()[3:] if row[0].strip()]
        products = fetch_product_details_by_codes(token, product_codes, {})
        update_sheet3(worksheet3, products)
        print("Данные успешно записаны в Лист3.")

    # Schedule the tasks
    schedule.every().day.at("00:10").do(process_sheet1, spreadsheet, token)
    schedule.every().day.at("00:13").do(process_sheet2, spreadsheet, token)
    schedule.every().day.at("00:16").do(update_sheet3_products)
    schedule.every().day.at("00:20").do(process_sheet3, spreadsheet, token)
    schedule.every().day.at("23:50").do(process_sheet5, spreadsheet, token)

    while True:
        schedule.run_pending()
        time.sleep(30)


def process_all_sheets():
    try:
        client = authenticate_google_sheets(config.CREDENTIALS_PATH)
        spreadsheet = client.open(config.SHEET_NAME)

        # Get MoySklad token
        #token = "6e54a61cf2e5bf885f343424ecc6facb267472c4"
        token = get_access_token(config.MS_USERNAME, config.MS_PASSWORD)
        print("Access Token:", token)

        # Process Sheet1
        process_sheet1(spreadsheet, token)

        # Process Sheet2
        process_sheet2(spreadsheet, token)

        # Обработка данных для Листа3
        try:
            worksheet3 = spreadsheet.worksheet("Лист3")
        except gspread.WorksheetNotFound:
            worksheet3 = spreadsheet.add_worksheet(title="Лист3", rows="1000", cols="3")
            print("Лист3 создан.")

        # Get product codes directly from Sheet3
        product_codes = [row[0] for row in worksheet3.get_all_values()[3:] if row[0].strip()]
        
        # Fetch product details from MoySklad
        products = fetch_product_details_by_codes(token, product_codes, {})
        
        # Update Sheet3 with fetched product details
        update_sheet3(worksheet3, products)
        print("Данные успешно записаны в Лист3.")

        # Process Sheet3
        process_sheet3(spreadsheet, token)

        # Process Sheet5
        try:
            worksheet5 = spreadsheet.worksheet("Лист5")
        except gspread.WorksheetNotFound:
            worksheet5 = spreadsheet.add_worksheet(title="Лист5", rows="1000", cols="20")
            print("Лист5 создан.")

        process_sheet5(worksheet5, token)

        # Schedule the tasks
        schedule_process_sheets(spreadsheet, token)

        return 0
    
    except Exception as e:
        print(f"Error occurred: {e}")
        return None
