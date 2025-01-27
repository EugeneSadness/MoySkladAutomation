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

def shift_data_left_and_add_new_values(row_data, orders_data, product_idx):
    """Сдвигает данные влево и добавляет новые значения"""
    row_data = row_data[2:] + [
        str(int(orders_data[product_idx].get('stock', 0))),
        str(int(orders_data[product_idx].get('orders_count', 0)))
    ]
    return row_data

def update_daily_stats_in_sheet(worksheet, orders_data: List[Dict], max_days: int = 90):
    # Получаем все данные с листа
    all_data = worksheet.get_all_values()
    header_row = all_data[4]

    print("Анализ заголовков:")
    date_col_map = {}
    
    # Перебираем все столбцы начиная с F (индекс 5)
    for col_idx in range(5, 184, 2):  # шаг 2, так как даты через столбец
        if col_idx >= len(header_row):
            break
            
        date_str = header_row[col_idx].strip()
        if date_str:
            try:
                print(f"Обработка даты из заголовка: {date_str}")
                header_date = datetime.strptime(date_str, "%d.%m.%Y")
                date_str_iso = header_date.strftime("%Y-%m-%d")
                # Сохраняем и колонку с датой, и колонку для остатка (на 1 левее)
                date_col_map[date_str_iso] = {
                    'orders_col': col_idx + 1,
                    'stock_col': col_idx
                }
                print(f"Добавлена дата: {date_str_iso} -> столбцы {get_column_letter(col_idx)} (остаток) и {get_column_letter(col_idx + 1)} (заказы)")
            except ValueError as e:
                print(f"Ошибка при обработке даты {date_str}: {e}")
                continue

    print(f"\nНайдено {len(date_col_map)} дат в заголовках:")
    for date, cols in sorted(date_col_map.items()):
        print(f"Дата: {date}, Столбец остатков: {get_column_letter(cols['stock_col'])}, Столбец заказов: {get_column_letter(cols['orders_col'])}")

    product_map = {item['code']: item for item in orders_data}
    updates = []

    # Перебираем строки с продуктами начиная с 6-й
    for row_idx in range(5, len(all_data)):
        row_values = all_data[row_idx]
        product_code = row_values[0].strip()

        if not product_code or product_code not in product_map:
            continue

        order_info = product_map[product_code]
        print(f"\nОбработка товара {product_code}")

        orders_by_date = order_info.get("orders_by_date", {})
        stock_by_date = order_info.get("stock_by_date", {})
        
        print(f"Найдено {len(orders_by_date)} дат заказов для товара")
        matches = 0
        
        for date_str, cols in date_col_map.items():
            # Обновляем количество заказов
            if date_str in orders_by_date:
                quantity = orders_by_date[date_str]
                order_cell = f'{get_column_letter(cols["orders_col"])}{row_idx+1}'
                updates.append({
                    'range': order_cell,
                    'values': [[int(quantity)]]
                })
                matches += 1

            # Обновляем остаток
            if date_str in stock_by_date:
                stock = stock_by_date[date_str]
                stock_cell = f'{get_column_letter(cols["stock_col"])}{row_idx+1}'
                updates.append({
                    'range': stock_cell,
                    'values': [[int(stock)]]
                })

        print(f"Найдено совпадений дат: {matches} из {len(orders_by_date)}")

    print(f"\nВсего подготовлено обновлений: {len(updates)}")
    
    if updates:
        worksheet.batch_update(updates)
        print("Обновление выполнено успешно")
    else:
        print("Нет данных для обновления")


def update_daily_stats_sliding_window(worksheet):
    # Получаем все данные с листа
    all_data = worksheet.get_all_values()
    header_row = all_data[4]
    
    print("Начало обработки sliding windows...")
    
    # Находим последнюю дату в заголовках (крайний правый столбец с датой)
    last_date_str = header_row[-1].strip()
    last_date = datetime.strptime(last_date_str, "%d.%m.%Y")
    
    # Вычисляем новую дату (следующий день)
    new_date = last_date + timedelta(days=1)
    new_date_str = new_date.strftime("%d.%m.%Y")
    
    print(f"Последняя дата: {last_date.strftime('%d.%m.%Y')}")
    print(f"Новая дата: {new_date_str}")
    
    # Формируем обновления для сдвига данных
    updates = []
    
    # Перебираем все строки начиная с 5-й (где данные)
    for row_idx, row in enumerate(all_data[4:], start=5):
        # Сдвигаем данные влево, начиная с колонки E (индекс 4)
        for col_idx in range(4, len(row)-2):  # -2 чтобы оставить место для новых столбцов
            cell_value = row[col_idx+2]  # Берем значение из столбца справа
            updates.append({
                'range': f'{get_column_letter(col_idx+1)}{row_idx}',
                'values': [[cell_value]]
            })
    
    # Добавляем новые столбцы справа
    last_col = len(header_row) - 1
    
    # Обновляем заголовки для новых столбцов
    updates.append({
        'range': f'{get_column_letter(last_col)}{5}',
        'values': [['Ост.']]
    })
    updates.append({
        'range': f'{get_column_letter(last_col+1)}{5}',
        'values': [[new_date_str]]
    })
    
    # Выполняем все обновления одним запросом
    if updates:
        worksheet.batch_update(updates)
        print("Обновление sliding windows выполнено успешно")




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
        print("end_row: ", end_row)
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
    dates_row = worksheet.row_values(2)[4:]  # G2 и правее
    
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

def sheet3_sliding_window(worksheet, num_dates: int = 180):
    # Получаем все значения одним запросом
    all_data = worksheet.get_all_values()
    header_row = all_data[1]  # Вторая строка с датами (строка 2)

    # Находим даты в заголовке, начиная с колонки E (индекс 4)
    dates = []
    date_cols = []  # Индексы колонок с датами
    for idx, cell in enumerate(header_row[4:], start=4):
        if cell.strip() and not cell.startswith('#'):
            dates.append(cell)
            date_cols.append(idx)

    # Если есть даты для сдвига
    if dates:
        rightmost_date = datetime.strptime(dates[-1], "%d.%m.%Y")
        new_date = (rightmost_date + timedelta(days=1)).strftime("%d.%m.%Y")

        updates = []
        
        # Получаем все формулы одним запросом
        formulas = worksheet.get('A1:ZZ', value_render_option='FORMULA')

        for row_idx, (data_row, formula_row) in enumerate(zip(all_data, formulas), start=1):
            if row_idx == 2:  # Строка с датами
                new_row = data_row[:4]
                new_row.extend([data_row[i] for i in date_cols[1:]])
                new_row.append(new_date)
            else:
                new_row = []
                # Первые 4 колонки
                for col_idx in range(4):
                    cell_value = formula_row[col_idx] if col_idx < len(formula_row) else ""
                    new_row.append(cell_value)
                
                # Значения для дат
                for col_idx in date_cols[1:]:
                    cell_value = formula_row[col_idx] if col_idx < len(formula_row) else ""
                    new_row.append(cell_value)
                
                new_row.append("")  # Пустая ячейка в конце

            range_name = f'A{row_idx}:{get_column_letter(len(new_row))}{row_idx}'
            updates.append({
                'range': range_name,
                'values': [new_row]
            })

        # Выполняем batch-обновление
        if updates:
            worksheet.batch_update(updates)

def update_supply_quantities_in_sheet3(worksheet, supplies_data: Dict[str, Dict[str, float]]):
    """
    Обновляет количества в приемках в Листе3 для будущих дат.
    
    Args:
        worksheet: Рабочий лист
        supplies_data: {код_товара: {дата: количество}}
    """
    # Получаем все значения
    all_values = worksheet.get_all_values()
    
    # Получаем даты из строки 2
    dates_row = worksheet.row_values(2)
    
    # Очищаем только данные о заказах, сохраняя даты
    # Начинаем с E4 (пропускаем заголовки и даты)
    # clear_range = f'E4:{get_column_letter(worksheet.col_count)}{len(all_values)}'
    # worksheet.batch_clear([clear_range])
    
    # Получаем все уникальные даты из supplies_data и сортируем их
    all_dates = set()
    for product_dates in supplies_data.values():
        all_dates.update(product_dates.keys())
    sorted_dates = sorted(list(all_dates))
    
    # Создаем словарь для маппинга дат к колонкам
    date_to_column = {}
    for idx, date_str in enumerate(dates_row[4:], start=0):  # Начинаем с E колонки (индекс 4)
        if date_str.strip():  # Пропускаем пустые ячейки
            date_to_column[date_str] = chr(69 + idx)  # E = 69 в ASCII
    
    # Обновляем количества
    value_updates = []
    format_updates = []
    for row_idx, row in enumerate(all_values[3:], start=4):
        product_code = row[0].strip()
        if product_code in supplies_data:
            product_dates = supplies_data[product_code]
            for date_str, quantity in product_dates.items():
                if date_str in date_to_column:
                    column_letter = date_to_column[date_str]
                    col_idx = ord(column_letter) - ord('A')
                    
                    # Получаем текущее значение напрямую из ячейки
                    cell = worksheet.cell(row_idx, col_idx + 1)  # +1 так как gspread использует индексацию с 1
                    current_value = cell.value if cell.value else ""
                    
                    # Если есть формула, добавляем к ней новое значение
                    if current_value.startswith('='):
                        new_value = f"{current_value}+{quantity}"
                    else:
                        # Если текущее значение числовое или пустое
                        try:
                            current_num = float(current_value) if current_value.strip() else 0
                            new_value = current_num + quantity
                        except (ValueError, AttributeError):
                            new_value = quantity
                    
                    cell_addr = f"{column_letter}{row_idx}"
                    value_updates.append({
                        'range': cell_addr,
                        'values': [[new_value]]
                    })
    
    # Применяем обновления батчем
    if value_updates:
        worksheet.batch_update(value_updates)
        print(f"Обновлены данные о приемках для {len(value_updates)} ячеек")

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


def update_sales_report_in_sheet5(worksheet, report: Dict[str, Dict[str, Dict[str, float]]], current_date: str):
    """
    Updates Sheet5 with the sales report data for multiple dates.

    Args:
        worksheet: Google Sheets worksheet for Sheet5.
        report: Dictionary with statuses as keys and channel/date amounts as values.
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

    # Get existing dates from header
    dates = get_dates_from_header(worksheet)

    # First clear existing values for all channels and dates
    clear_updates = []
    for (status, channel), row in channel_rows.items():
        for date_str in dates:
            date_col = dates.index(date_str) + 2  # +2 because we start from column B
            clear_updates.append({
                'range': f'{get_column_letter(date_col)}{row}',
                'values': [['']]  # Clear cell by setting empty value
            })

    if clear_updates:
        worksheet.batch_update(clear_updates)

    # Prepare batch updates with new values
    updates = []
    for status, channels in report.items():
        for channel, date_amounts in channels.items():
            row = channel_rows.get((status, channel))
            if row:
                for date_str, amount in date_amounts.items():
                    if date_str not in dates:
                        continue

                    date_col = dates.index(date_str) + 2  # +2 because we start from column B
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


def update_categories_costs_in_sheet5(worksheet, categories_costs: Dict[str, float]):
    """
    Обновляет суммарную себестоимость товаров по категориям в Листе5 после спец. знака '\'

    Args:
        worksheet: Рабочий лист Google Sheets
        categories_costs: Словарь с суммами по категориям {категория: сумма}
    """
    # Получаем текущую дату
    current_date = datetime.now().strftime("%d.%m.%Y")

    # Получаем все даты из заголовка, игнорируя столбцы со знаком #
    header_row = worksheet.row_values(1)
    dates = []
    date_cols = []  # Сохраняем индексы столбцов с датами
    for idx, cell in enumerate(header_row[1:], start=2):  # Начинаем с B (индекс 2)
        if cell.strip() and not cell.startswith('#'):
            dates.append(cell.strip())
            date_cols.append(idx)

    # Определяем колонку для текущей даты
    if current_date not in dates:
        # Если текущей даты нет, добавляем новую колонку
        new_column = date_cols[-1] + 1 if date_cols else 2
        worksheet.update_cell(1, new_column, current_date)
        date_col = new_column
    else:
        date_col = date_cols[dates.index(current_date)]

    # Находим строку с "Остатки"
    col_a_values = worksheet.col_values(1)
    try:
        start_row = col_a_values.index('Остатки') + 1
    except ValueError:
        print("Название 'Остатки' не найдено в столбце A")
        return

    # Получаем все категории из столбца A до строки "Заказано В пути", игнорируя строки со знаком #
    categories_in_sheet = []
    for cell in col_a_values[start_row:]:
        if cell.strip() == "Заказано В пути":
            break
        if cell.strip() and not cell.startswith('#'):
            categories_in_sheet.append(cell.strip())

    # Подготавливаем обновления
    updates = []
    current_row = start_row

    # Обновляем значения только для существующих категорий
    for category in categories_in_sheet:
        cost = categories_costs.get(category, 0.0)
        cell = gspread.Cell(current_row + 1, date_col, f"{cost:.2f}")  # Используем колонку с текущей датой
        updates.append(cell)
        current_row += 1

    if updates:
        try:
            worksheet.update_cells(updates)
            print(f"Обновлены данные о себестоимости для {len(categories_costs)} категорий")
        except Exception as e:
            print(f"Ошибка при обновлении данных о себестоимости: {str(e)}")
            raise

def update_transits_costs_in_sheet5(worksheet, categories_costs: Dict[str, float]):
    """
    Обновляет суммарную себестоимость товаров по категориям в Листе5 после спец. знака '/'

    Args:
        worksheet: Рабочий лист Google Sheets
        categories_costs: Словарь с суммами по категориям {категория: сумма}
    """
    # Получаем текущую дату
    current_date = datetime.now().strftime("%d.%m.%Y")

    # Получаем все даты из заголовка, игнорируя столбцы со знаком #
    header_row = worksheet.row_values(1)
    dates = []
    date_cols = []  # Сохраняем индексы столбцов с датами
    for idx, cell in enumerate(header_row[1:], start=2):  # Начинаем с B (индекс 2)
        if cell.strip() and not cell.startswith('#'):
            dates.append(cell.strip())
            date_cols.append(idx)

    # Определяем колонку для текущей даты
    if current_date not in dates:
        # Если текущей даты нет, добавляем новую колонку
        new_column = date_cols[-1] + 1 if date_cols else 2
        worksheet.update_cell(1, new_column, current_date)
        date_col = new_column
    else:
        date_col = date_cols[dates.index(current_date)]

    # Находим строку с "Заказано В пути"
    col_a_values = worksheet.col_values(1)
    try:
        start_row = col_a_values.index('Заказано В пути') + 1
    except ValueError:
        print("Название 'Заказано В пути' не найдено в столбце A")
        return

    # Получаем все категории из столбца A после строки "Заказано В пути", игнорируя строки со знаком #
    categories_in_sheet = []
    for cell in col_a_values[start_row:]:
        if not cell.strip():
            break
        if cell.strip() and not cell.startswith('#'):
            categories_in_sheet.append(cell.strip())

    # Подготавливаем обновления
    updates = []
    current_row = start_row

    # Обновляем значения только для существующих категорий
    for category in categories_in_sheet:
        cost = categories_costs.get(category, 0.0)
        cell = gspread.Cell(current_row + 1, date_col, f"{cost:.2f}")  # Используем колонку с текущей датой
        updates.append(cell)
        current_row += 1

    if updates:
        try:
            worksheet.update_cells(updates)
            print(f"Обновлены данные о товарах в пути для {len(categories_costs)} категорий")
        except Exception as e:
            print(f"Ошибка при обновлении данных о товарах в пути: {str(e)}")
            raise



def update_daily_stats_in_sheet5_sliding_window(worksheet, num_dates: int = 180):
    """
    Сдвигает все колонки влево в Sheet5, удаляя крайнюю левую дату и добавляя новую дату справа.
    Ближайшая дата всегда слева, дальняя справа.

    Args:
        worksheet: Рабочий лист Google Sheets
        num_dates: Количество дат для отображения
    """
    # Получаем все значения
    all_data = worksheet.get_all_values()
    header_row = all_data[0]  # Первая строка с датами

    # Находим даты в заголовке
    dates = []
    date_cols = []  # Индексы колонок с датами
    for idx, cell in enumerate(header_row[1:], start=1):  # Пропускаем первую колонку
        if cell.strip() and not cell.startswith('#'):
            dates.append(cell)
            date_cols.append(idx)

    # Если есть даты для сдвига
    if dates:
        # Получаем самую правую дату и преобразуем ее
        rightmost_date = datetime.strptime(dates[-1], "%d.%m.%Y")
        # Добавляем один день к самой правой дате
        new_date = (rightmost_date + timedelta(days=1)).strftime("%d.%m.%Y")

        updates = []

        # Для каждой строки
        for row_idx, row in enumerate(all_data, start=1):
            new_row = [row[0]]  # Первая колонка (A)

            if row_idx == 1:  # Для заголовка
                # Добавляем все существующие даты, кроме первой
                new_row.extend([row[i] for i in date_cols[1:]])
                # Добавляем новую дату в конец
                new_row.append(new_date)
            else:  # Для остальных строк
                # Добавляем все значения, кроме первого
                new_row.extend([row[i] for i in date_cols[1:]])
                # Добавляем пустую ячейку в конец
                new_row.append("")

            # Добавляем обновление для этой строки
            range_name = f'A{row_idx}:{get_column_letter(len(new_row))}{row_idx}'
            updates.append({
                'range': range_name,
                'values': [new_row]
            })

        # Выполняем batch-обновление
        if updates:
            worksheet.batch_update(updates)




def fill_dates_sheet5(worksheet, days: int = 180):
    """
    Заполняет Sheet5 датами на заданное количество дней вперед.
    Даты располагаются слева направо, от ближайшей к самой дальней.

    Args:
        worksheet: Рабочий лист Google Sheets (Sheet5)
        days: Количество дней для генерации дат (по умолчанию 180)
    """
    try:
        # Устанавливаем конечную дату (13.12.2024)
        end_date = datetime(2024, 12, 13).date()

        # Вычисляем начальную дату (на указанное количество дней вперед от конечной)
        start_date = end_date + timedelta(days=days)

        # Генерируем список дат
        dates = []
        current_date = end_date
        while current_date <= start_date:
            dates.append(current_date.strftime("%d.%m.%Y"))
            current_date += timedelta(days=1)

        # Подготавливаем обновление для первой строки
        # Начинаем с колонки B (индекс 2)
        update_range = f'B1:{get_column_letter(len(dates) + 1)}1'

        # Обновляем даты в таблице
        worksheet.update(update_range, [dates])

        print(f"Даты успешно заполнены. Добавлено {len(dates)} дат с {dates[0]} по {dates[-1]}")

    except Exception as e:
        print(f"Ошибка при заполнении дат: {str(e)}")
        raise

