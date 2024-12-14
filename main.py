from datetime import datetime

import gspread

from auth.google_auth import authenticate_google_sheets
from auth.moysklad_auth import get_access_token
from services.google_sheets_handler import (
    get_product_codes_from_sheet, update_product_details_in_sheet,
    get_products_with_details, update_daily_stats_in_sheet,
    get_product_codes_from_sheet2, get_products_with_details_sheet2,
    update_product_details_in_sheet2, update_daily_stats_in_sheet2,
    update_sheet3, get_supply_dates_from_sheet3, update_supply_quantities_in_sheet3,
    get_sales_channels_and_statuses, update_sales_report_in_sheet5, update_categories_costs_in_sheet5,
    update_daily_stats_in_sheet5_sliding_window, fill_dates_sheet5
)
from services.moysklad_api import (
    fetch_product_details_by_codes, fetch_customer_orders_for_products,
    fetch_supplies_by_date_range, fetch_orders_by_channels, fetch_categories_costs
)
from utils.date_handler import get_current_day_date_range


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

def process_sheet3(spreadsheet, token):
    """Обрабатывает данные приемок для Листа3 для будущих дат"""
    try:
        worksheet3 = spreadsheet.worksheet("Лист3")
        
        # Получаем коды товаров и будущие даты
        supply_dates = get_supply_dates_from_sheet3(worksheet3)
        
        if not supply_dates:
            print("Нет данных для обработки будущих приемок")
            return
            
        # Получаем приемки начиная с текущей даты
        current_date = datetime.now().strftime("%Y-%m-%d")
        supplies = fetch_supplies_by_date_range(token, current_date)
        
        # Формируем словарь {код_товара: {дата_приемки: количество}}
        supplies_quantities = {}
        
        for supply in supplies:
            try:
                # Об��езаем миллисекунды и обрабатываем дату
                moment = supply['moment'].split('.')[0]  # Убираем миллисекунды
                supply_date = datetime.strptime(moment, "%Y-%m-%d %H:%M:%S").strftime("%d.%m.%Y")
                
                for position in supply['positions']:
                    code = position['code']
                    if code in supply_dates and supply_date in supply_dates[code]:
                        if code not in supplies_quantities:
                            supplies_quantities[code] = {}
                        
                        # Суммируем количество для одной даты
                        if supply_date not in supplies_quantities[code]:
                            supplies_quantities[code][supply_date] = 0
                        supplies_quantities[code][supply_date] += position['quantity']
            except ValueError as e:
                print(f"Ошибка обработки даты для приемки {supply.get('id')}: {e}")
                continue
        
        # Обновляем данные в таблице
        if supplies_quantities:
            update_supply_quantities_in_sheet3(worksheet3, supplies_quantities)
            print(f"Данные о будущих приемках обновлены в Лист3")
        else:
            print("Нет данных о будущих приемках")
            
    except Exception as e:
        print(f"Ошибка при обработке Лист3: {e}")
        raise

def process_sheet5(worksheet, token):
    """
    Обрабатывает Лист5: обновляет статистику по заказам и остаткам по категориям
    """
    try:
        fill_dates_sheet5(worksheet)
        # Сдвигаем все колонки влево в Sheet5, освобождая место для новых данных
        #update_daily_stats_in_sheet5_sliding_window(worksheet)

        # 1. Получаем статусы и каналы продаж из таблицы
        status_channels = get_sales_channels_and_statuses(worksheet)
        
        # 2. Получаем данные о заказах по каналам (до знака /)
        orders_report = fetch_orders_by_channels(token, status_channels)
        
        # 3. Получаем данные об остатках по категориям (после знака /)
        categories_costs = fetch_categories_costs(token)
        
        # 4. Обновляем данные о заказах (до знака /)
        current_date = datetime.now().strftime("%d.%m.%Y")
        update_sales_report_in_sheet5(worksheet, orders_report, current_date)
        
        # 5. Обновляем данные об остатках по категориям (после знака /)
        update_categories_costs_in_sheet5(worksheet, categories_costs)
        
        print("Лист5 успешно обновлен")
        
    except Exception as e:
        print(f"Ошибка при обработке Лист5: {str(e)}")
        raise

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
        print("Access Token:", token)

        # Process Sheet1 (original functionality)
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

        # Process Sheet2
        process_sheet2(client, spreadsheet, token)

        # Обработка данных для Листа3
        try:
            worksheet3 = spreadsheet.worksheet("Лист3")
        except gspread.WorksheetNotFound:
            worksheet3 = spreadsheet.add_worksheet(title="Лист3", rows="1000", cols="3")
            print("Лист3 создан.")

        # Объединяем продукты из Sheet1 и Sheet2
        combined_products = {**existing_products, **get_products_with_details_sheet2(spreadsheet.worksheet("Лист2"))}

        # Обновляем Лист3
        update_sheet3(worksheet3, combined_products)
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

        return orders_data
    
    except Exception as e:
        print(f"Error occurred: {e}")
        return None

if __name__ == "__main__":
    main() 