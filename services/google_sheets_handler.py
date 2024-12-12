from typing import List, Dict
from datetime import datetime, timedelta
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

def update_sheet3(worksheet, products: Dict[str, Dict], start_row: int = 3):
    """
    Заполняет Лист3 данными о товарах: Код (A), Товар (B), Название (C).
    
    Args:
        worksheet: Объект рабочего листа Google Sheets для Лист3
        products: Словарь с данными о продуктах
        start_row: Начальная строка для заполнения (по умолчанию 3)
    """
    try:
        data = []
        headers = ["Код", "Товар", "Название"]
        data.append(headers)
        
        for idx, (code, details) in enumerate(products.items(), start=start_row):
            row = [
                code,
                details.get("category", ""),
                details.get("name", "")
            ]
            data.append(row)
        
        # Определяем диапазон для обновления
        end_row = start_row + len(products)
        range_name = f'A{start_row}:C{end_row}'
        
        # Обновляем данные в Лист3
        worksheet.update(range_name, data)
        
        print(f"Лист3 обновлен: {len(products)} записей добавлено.")
        
    except Exception as e:
        print(f"Ошибка при обновлении Лист3: {str(e)}")
        raise

def update_sheet3_acceptances(worksheet, acceptance_data: Dict[str, Dict[datetime, int]], 
                              date_headers: List[datetime], start_row: int = 4):
    """
    Обновляет Лист3 данными о приемках товаров на конкретные даты.

    Args:
        worksheet: Объект рабочего листа Google Sheets для Лист3
        acceptance_data (Dict[str, Dict[datetime, int]]): Словарь {код товара: {дата: количество}}
        date_headers (List[datetime]): Список дат, соответствующих заголовкам столбцов
        start_row (int): Начальная строка для обновления данных (по умолчанию 4)
    """
    try:
        # Получаем текущие данные из Листа3
        existing_data = worksheet.get_all_values()
        
        # Создаем маппинг кодов товаров к строкам
        code_to_row = {}
        for idx, row in enumerate(existing_data[start_row-1:], start=start_row):
            if len(row) >= 1:
                code = row[0].strip()
                if code:
                    code_to_row[code] = idx

        # Подготавливаем данные для обновления
        updates = []
        for code, date_counts in acceptance_data.items():
            row = code_to_row.get(code)
            if not row:
                # Если код товара не найден, пропускаем
                continue
            for date in date_headers:
                col = get_column_number('G') + (date - date_headers[0]).days  # Предполагая, что G2 - первая дата
                cell = gspread.utils.rowcol_to_a1(row, col)
                quantity = date_counts.get(date, 0)
                updates.append({
                    'range': cell,
                    'values': [[quantity]]
                })

        # Группируем обновления по диапазонам
        for update in updates:
            worksheet.update(update['range'], update['values'])

        print(f"Лист3 обновлен данными о приемках.")
        
    except Exception as e:
        print(f"Ошибка при обновлении приемок Лист3: {str(e)}")
        raise

def get_column_number(column_letter: str) -> int:
    """
    Преобразует буквенное обозначение столбца в номер.

    Args:
        column_letter (str): Буквенное обозначение столбца (например, 'A')

    Returns:
        int: Номер столбца
    """
    num = 0
    for c in column_letter.upper():
        num = num * 26 + (ord(c) - ord('A') + 1)
    return num

def get_supply_dates_from_sheet3(worksheet) -> Dict[str, List[str]]:
    """
    Получает все будущие даты приемок из Листа3.
    
    Returns:
        Dict[str, List[str]]: {код_товара: [будущие_даты]}
    """
    # Получаем текущую дату
    current_date = datetime.now().date()
    
    # Получаем заголовки с датами (начиная с G2)
    dates_row = worksheet.row_values(2)[6:]  # G2 и правее
    
    # Фильтруем только будущие даты
    future_dates = []
    future_date_indices = []  # Индексы для определения колонок
    for idx, date_str in enumerate(dates_row):
        if not date_str.strip():
            continue
        try:
            date = datetime.strptime(date_str, "%d.%m.%Y").date()
            if date >= current_date:
                future_dates.append(date_str)
                future_date_indices.append(idx)
        except ValueError:
            continue
    
    # Получаем коды товаров
    all_values = worksheet.get_all_values()
    product_supplies = {}
    
    # Начинаем с 4-й строки
    for row in all_values[3:]:
        if not row[0].strip():  # Пропускаем пустые строки
            continue
            
        product_code = row[0].strip()
        product_supplies[product_code] = future_dates
    
    return product_supplies

def update_supply_quantities_in_sheet3(worksheet, supplies_data: Dict[str, Dict[str, float]]):
    """
    Обновляет количества в приемках в Листе3 для будущих дат.
    
    Args:
        worksheet: Рабочий лист
        supplies_data: {код_товара: {дата: количество}}
    """
    # Получаем заголовки с датами
    dates_row = worksheet.row_values(2)[6:]  # G2 и правее
    
    # Создаем словарь для маппинга дат к колонкам
    date_to_column = {}
    for idx, date_str in enumerate(dates_row):
        if date_str.strip():
            column_letter = chr(71 + idx)  # G = 71 в ASCII
            date_to_column[date_str] = column_letter
    
    # Получаем все значения
    all_values = worksheet.get_all_values()
    
    # Обновляем количества
    updates = []
    for row_idx, row in enumerate(all_values[3:], start=4):  # Начинаем с 4-й строки
        product_code = row[0].strip()
        if product_code in supplies_data:
            product_dates = supplies_data[product_code]
            for date_str, quantity in product_dates.items():
                if date_str in date_to_column:
                    column_letter = date_to_column[date_str]
                    cell = f"{column_letter}{row_idx}"
                    updates.append({
                        'range': cell,
                        'values': [[quantity]]
                    })
    
    # Применяем обновления батчем
    if updates:
        worksheet.batch_update(updates)
        print(f"Обновлены данные о приемках для {len(updates)} ячеек")

def get_sales_channels_and_statuses(worksheet) -> Dict[str, List[str]]:
    """
    Gets sales channel names grouped by their statuses from column A.
    
    Args:
        worksheet: Google Sheets worksheet for Sheet5.
    
    Returns:
        Dict[str, List[str]]: Dictionary with statuses as keys and lists of channels as values.
    """
    status_channels = {}
    current_status = None
    
    for cell in worksheet.col_values(1):
        cell = cell.strip()
        if cell.startswith('\\'):
            break
        if not cell or cell.startswith('#'):
            continue
            
        if cell.startswith('(') and cell.endswith(')'):
            current_status = cell
            status_channels[current_status] = []
        elif current_status and cell:
            status_channels[current_status].append(cell)
            
    return status_channels

def update_sales_report_in_sheet5(worksheet, report: Dict[str, Dict[str, float]], current_date: str):
    """
    Updates Sheet5 with the sales report data for the current date.
    
    Args:
        worksheet: Google Sheets worksheet for Sheet5.
        report: Dictionary with statuses as keys and channel amounts as values.
        current_date: Current date in format dd.mm.yyyy
    """
    # Get all statuses and channels with their row numbers
    channel_rows = {}
    current_status = None
    
    for idx, cell in enumerate(worksheet.col_values(1), start=1):
        cell = cell.strip()
        if cell.startswith('\\'):
            break
        if not cell or cell.startswith('#'):
            continue
            
        if cell.startswith('(') and cell.endswith(')'):
            current_status = cell
        elif current_status and cell:
            channel_rows[(current_status, cell)] = idx
    
    # Get or create date column
    dates = get_dates_from_header(worksheet)
    if current_date not in dates:
        new_column = len(dates) + 2  # +2 because we start from column B
        worksheet.update_cell(1, new_column, current_date)
        date_col = new_column
    else:
        date_col = dates.index(current_date) + 2
    
    # Prepare batch updates
    updates = []
    for status, channels in report.items():
        for channel, amount in channels.items():
            row = channel_rows.get((status, channel))
            if row:
                updates.append({
                    'range': f'{get_column_letter(date_col)}{row}',
                    'values': [[amount]]
                })
    
    if updates:
        worksheet.batch_update(updates)

def get_dates_from_header(worksheet) -> List[str]:
    """
    Retrieves all dates from the header row (row 1), starting from column B.

    Args:
        worksheet: Google Sheets worksheet for Sheet5.

    Returns:
        List[str]: List of date strings in format dd-mm-yyyy.
    """
    header = worksheet.row_values(1)[1:]  # Skip column A
    dates = [date.strip() for date in header if date.strip()]
    return dates

def update_category_costs(worksheet, orders_data: List[Dict], start_row: int):
    """
    Обновляет суммарную себестоимость товаров по категориям на текущую дату.
    
    Args:
        worksheet: Рабочий лист Google Sheets
        orders_data: Список словарей с данными о товарах
        start_row: Строка, с которой начинаются категории (после спецзнака /)
    """
    # Получаем все категории из столбца A, начиная с указанной строки
    categories = worksheet.col_values(1)[start_row-1:]
    
    # Группируем товары по категориям и считаем общую стоимость
    category_costs = {}
    for item in orders_data:
        category = item.get('category', 'Без категории')
        stock = float(item.get('stock', 0))
        cost = float(item.get('buy_price', 0))  # Предполагаем, что у нас есть закупочная цена
        
        if category not in category_costs:
            category_costs[category] = 0
        category_costs[category] += stock * cost

    # Получаем текущую дату
    current_date = datetime.now().strftime("%d.%m.%Y")
    
    # Находим или создаем столбец для текущей даты
    all_dates = worksheet.row_values(start_row-1)  # Получаем заголовки столбцов
    if current_date in all_dates:
        date_col = all_dates.index(current_date) + 1
    else:
        # Находим первый пустой столбец
        date_col = len(all_dates) + 1
        # Добавляем дату в заголовок
        worksheet.update_cell(start_row-1, date_col, current_date)

    # Обновляем данные по категориям
    cells_to_update = []
    for idx, category in enumerate(categories, start=start_row):
        if category.strip():  # Пропускаем пустые строки
            cost = category_costs.get(category, 0)
            cells_to_update.append(
                gspread.Cell(idx, date_col, str(round(cost, 2)))
            )

    if cells_to_update:
        worksheet.update_cells(cells_to_update)