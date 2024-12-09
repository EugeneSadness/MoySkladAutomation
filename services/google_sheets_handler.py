from typing import List, Dict
from datetime import datetime
import string
import gspread

def get_column_letter(column_number):
    """Преобразует номер столбца в буквенное обозначение"""
    result = ""
    while column_number > 0:
        column_number -= 1
        result = string.ascii_uppercase[column_number % 26] + result
        column_number //= 26
    return result

def get_product_codes_from_sheet(worksheet) -> List[str]:
    codes = worksheet.col_values(1)
    return [code for code in codes[5:] if code.strip()] 

def get_products_with_details(worksheet, start_row: int = 6) -> Dict[str, Dict]:
    """
    Gets information about products that are already filled in the table.
    
    Returns:
        Dict[str, Dict]: {code: {name, category, description}}
    """
    # Get all values from relevant columns
    all_values = worksheet.get_all_values()
    
    # Skip header rows (first 5 rows)
    data_rows = all_values[start_row-1:]
    
    products = {}
    
    for row in data_rows:
        if len(row) >= 4:  # Ensure row has enough columns
            code = row[0].strip()
            if not code:
                continue
                
            # If at least the name is filled, consider the product info complete
            if len(row) >= 3 and row[2].strip():
                products[code] = {
                    "name": row[2].strip(),
                    "category": row[1].strip() if len(row) >= 2 else "",
                    "description": row[3].strip() if len(row) >= 4 else ""
                }
    
    return products

def update_product_details_in_sheet(worksheet, products: Dict[str, Dict], start_row: int = 6):
    """
    Updates only unfilled product information in the sheet using batch updates.
    """
    codes = worksheet.col_values(1)
    existing_products = get_products_with_details(worksheet)
    
    # Create a list for batch updates
    cells_to_update = []
    
    for idx, code in enumerate(codes[start_row-1:], start=start_row):
        if not code.strip() or code in existing_products:
            continue
            
        product = products.get(code, {})
        if product:
            # Create gspread Cell objects for each update
            cells_to_update.extend([
                gspread.Cell(idx, 2, product.get('category', '')),   # Column B
                gspread.Cell(idx, 3, product.get('name', '')),       # Column C
                gspread.Cell(idx, 4, product.get('description', '')) # Column D
            ])
    
    # Perform batch update if there are cells to update
    if cells_to_update:
        worksheet.update_cells(cells_to_update)

def update_daily_stats_in_sheet(worksheet, orders_data: List[Dict], max_days: int = 90):
    """
    Обновляет статистику по остаткам и заказам в формате скользящего окна.
    
    Args:
        worksheet: Рабочий лист Google Sheets
        orders_data: Список словарей с данными о заказах и остатках
        max_days: Максимальное количество дней для хранения (по умолчанию 90)
    """
    # Получаем текущую дату в нужном формате
    current_date = datetime.now().strftime("%d.%m.%y")
    
    # Находим начальную колонку для статистики (E - остаток, F - дата)
    stats_start_col = 5  # Колонка E
    
    # Получаем все существующие данные
    all_data = worksheet.get_all_values()
    header_row = all_data[4]  # Строка с заголовками (индекс 4 = строка 5)
    
    # Получаем заголовки дат (каждый второй столбец начиная с F)
    dates = header_row[stats_start_col+1::2]
    dates = [d for d in dates if d.strip()]
    
    # Если текущей даты нет в заголовках
    if current_date not in dates:
        updates = []  # список обновлений для batch-запроса
        
        # Обрабатываем заголовки
        headers_update = header_row[stats_start_col-1:]  # Получаем все заголовки начиная с E
        if dates:  # Если есть существующие даты
            # Сдвигаем заголовки влево и добавляем новые
            headers_update = headers_update[2:] + ['Ост', current_date]
            
            # Обрабатываем данные для каждой строки
            for row_idx, row in enumerate(all_data[5:], start=6):  # Начинаем с 6-й строки
                row_data = row[stats_start_col-1:]  # Получаем данные начиная с колонки E
                # Сдвигаем данные влево и добавляем новые значения
                product_idx = row_idx - 6
                if product_idx < len(orders_data):
                    row_data = row_data[2:] + [
                        str(int(orders_data[product_idx].get('stock', 0))),
                        str(int(orders_data[product_idx].get('orders_count', 0)))
                    ]
                else:
                    row_data = row_data[2:] + ['', '']
                
                # Добавляем обновление для этой строки
                range_name = f'{get_column_letter(stats_start_col)}{row_idx}:{get_column_letter(stats_start_col + len(row_data) - 1)}{row_idx}'
                updates.append({
                    'range': range_name,
                    'values': [row_data]
                })
        
        # Обновляем заголовки
        header_range = f'{get_column_letter(stats_start_col)}5:{get_column_letter(stats_start_col + len(headers_update) - 1)}5'
        updates.insert(0, {
            'range': header_range,
            'values': [headers_update]
        })
        
        # Выполняем batch-обновление
        worksheet.batch_update(updates)

def get_product_codes_from_sheet2(worksheet) -> List[str]:
    """Gets product codes from Sheet2 starting from C4"""
    codes = worksheet.col_values(3)  # Column C
    return [code for code in codes[3:] if code.strip()]

def get_products_with_details_sheet2(worksheet, start_row: int = 4) -> Dict[str, Dict]:
    """
    Gets information about products that are already filled in Sheet2.
    
    Returns:
        Dict[str, Dict]: {code: {category, product_type, name}}
    """
    all_values = worksheet.get_all_values()
    data_rows = all_values[start_row-1:]
    
    products = {}
    
    for row in data_rows:
        if len(row) >= 4:  # Ensure row has enough columns
            code = row[2].strip()  # Column C
            if not code:
                continue
                
            # If name is filled, consider the product info complete
            if row[3].strip():
                products[code] = {
                    "category": row[0].strip(),      # Column A
                    "product_type": row[1].strip(),  # Column B
                    "name": row[3].strip()           # Column D
                }
    
    return products

def update_product_details_in_sheet2(worksheet, products: Dict[str, Dict], start_row: int = 4):
    """Updates unfilled product information in Sheet2"""
    codes = worksheet.col_values(3)  # Column C
    existing_products = get_products_with_details_sheet2(worksheet)
    
    cells_to_update = []
    
    for idx, code in enumerate(codes[start_row-1:], start=start_row):
        if not code.strip() or code in existing_products:
            continue
            
        product = products.get(code, {})
        if product:
            cells_to_update.extend([
                gspread.Cell(idx, 1, product.get('category', '')),     # Column A
                gspread.Cell(idx, 2, product.get('product_type', '')), # Column B
                gspread.Cell(idx, 4, product.get('name', ''))          # Column D
            ])
    
    if cells_to_update:
        worksheet.update_cells(cells_to_update)

def update_daily_stats_in_sheet2(worksheet, orders_data: List[Dict], start_row: int = 4):
    """Updates stock and orders statistics in Sheet2 and updates the report date"""
    # Update the current date in E2:E3
    current_date = datetime.now().strftime("%d.%m.%Y")
    date_cells = [
        gspread.Cell(2, 5, current_date),  # E2
        gspread.Cell(3, 5, current_date)   # E3
    ]
    
    # Get codes from Column C
    codes = worksheet.col_values(3)[start_row-1:]
    
    cells_to_update = date_cells.copy()  # Start with date cells
    
    for idx, code in enumerate(codes, start=start_row):
        if not code.strip():
            continue
            
        # Find matching order data
        order_info = next((item for item in orders_data if item['code'] == code), None)
        if order_info:
            cells_to_update.extend([
                gspread.Cell(idx, 5, str(int(order_info.get('stock', 0)))),        # Column E
                gspread.Cell(idx, 6, str(int(order_info.get('orders_count', 0))))  # Column F
            ])
    
    if cells_to_update:
        worksheet.update_cells(cells_to_update)