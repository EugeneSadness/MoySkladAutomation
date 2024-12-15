from typing import List, Dict
import requests
from datetime import datetime, timedelta

from utils.error_handler import print_api_errors


def fetch_products_by_codes(access_token: str, product_codes: List[str]) -> List[Dict]:
    url = "https://api.moysklad.ru/api/remap/1.2/entity/product"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept-Encoding": "gzip"
    }
    
    products = []
    for code in product_codes:
        try:
            params = {"filter": f"code={code}"}
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()
            
            if data.get("rows"):
                products.extend(data["rows"])
        except requests.HTTPError as e:
            print_api_errors(e.response)
            raise e
    
    return products

def fetch_product_details_by_codes(access_token: str, product_codes: List[str], existing_products: Dict[str, Dict]) -> Dict[str, Dict]:
    """
    Получает информацию только о новых товарах.
    """
    # Фием коды товаров, оставляя только те, для которых нет информации
    new_codes = [code for code in product_codes if code not in existing_products]
    
    if not new_codes:
        return existing_products
    
    url = "https://api.moysklad.ru/api/remap/1.2/entity/product"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept-Encoding": "gzip"
    }
    
    products_dict = existing_products.copy()
    
    for code in new_codes:
        try:
            params = {"filter": f"code={code}"}
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()
            
            if data.get("rows"):
                product = data["rows"][0]
                products_dict[code] = {
                    "id": product.get("id"),
                    "name": product.get("name"),
                    "category": product.get("productFolder", {}).get("name", "Без категории"),
                    "description": product.get("description", ""),
                    "article": product.get("article", ""),
                    "meta": product.get("meta")
                }
        except requests.HTTPError as e:
            print_api_errors(e.response)
            raise e
    
    return products_dict

def fetch_customer_orders_for_products(access_token: str, start_date: str, end_date: str, products: Dict[str, Dict]) -> List[Dict]:
    
    url = "https://api.moysklad.ru/api/remap/1.2/entity/customerorder"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept-Encoding": "gzip"
    }
    
    params = {
        "filter": f"moment>={start_date};moment<={end_date}",
        "limit": 100,
        "expand": "positions"
    }
    
    product_stats = {code: {"count": 0} | details for code, details in products.items()}
    product_cache = {}
    
    offset = 0
    while True:
        params['offset'] = offset
        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            orders = response.json().get("rows", [])
            
            if not orders:
                break
            
            for order in orders:
                positions = order.get("positions", {}).get("rows", [])
                
                for position in positions:
                    assortment = position.get("assortment", {})
                    product_href = assortment.get("meta", {}).get("href")
                    
                    if product_href not in product_cache:
                        product_response = requests.get(product_href, headers=headers)
                        product_response.raise_for_status()
                        product_data = product_response.json()
                        product_cache[product_href] = product_data.get("code")
                    
                    product_code = product_cache[product_href]
                    
                    if product_code in product_stats:
                        quantity = float(position.get("quantity", 0))
                        product_stats[product_code]["count"] += quantity
        
        except requests.HTTPError as e:
            print_api_errors(e.response)
            raise e
        
        offset += len(orders)
        if len(orders) < 100:
            break
    
    result = []
    stocks = fetch_product_stock(access_token, list(product_stats.keys()))
    
    for code, stats in product_stats.items():
        result.append({
            "code": code,
            "name": stats.get("name", ""),
            "category": stats.get("category", ""),
            "description": stats.get("description", ""),
            "orders_count": int(stats["count"]),
            "stock": stocks.get(code, 0)
        })
    
    return result 

def fetch_product_stock(access_token: str, product_codes: List[str]) -> Dict[str, float]:
    china_transit_url = fetch_url_stock_CHINA_in_transit(access_token)
    """
    Получает физические остатки для списка товаров.
    
    Args:
        access_token (str): Токен доступа
        product_codes (List[str]): Список кодов товаров
        
    Returns:
        Dict[str, float]: Словарь {код товара: физический остаток}
    """
    url = "https://api.moysklad.ru/api/remap/1.2/report/stock/all"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept-Encoding": "gzip"
    }
    
    params = {
        "filter": f"store!={china_transit_url}"
    }
    
    stock_dict = {}
    
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        
        for item in data.get("rows", []):
            code = item.get("code")
            if code in product_codes:
                stock_dict[code] = float(item.get("stock", 0))
                
    except requests.HTTPError as e:
        print_api_errors(e.response)
        raise e
        
    return stock_dict 

def fetch_supplies_by_date_range(access_token: str, start_date: str) -> List[Dict]:
    china_transit_url = fetch_url_stock_CHINA_in_transit(access_token)
    """
    Получает список приемок за указанный период.
    
    Args:
        access_token (str): Токен доступа
        start_date (str): Начальная дата в формате YYYY-MM-DD
        end_date (str): Конечная дата в формате YYYY-MM-DD
        
    Returns:
        List[Dict]: Список приемок с их позициями
    """
    url = "https://api.moysklad.ru/api/remap/1.2/entity/supply"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept-Encoding": "gzip"
    }
    
    params = {
        "filter": f"moment>{start_date} 00:00:00",
        "limit": 100,
        "expand": "positions",
        "filter": f"store!={china_transit_url}"
    }
    
    supplies = []
    product_cache = {}
    
    offset = 0
    while True:
        params['offset'] = offset
        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()
            supply_rows = data.get("rows", [])
            
            if not supply_rows:
                break
                
            for supply in supply_rows:
                positions = supply.get("positions", {}).get("rows", [])
                supply_positions = []
                
                for position in positions:
                    assortment = position.get("assortment", {})
                    product_href = assortment.get("meta", {}).get("href")
                    
                    # Получаем информацию о товаре из кэша или через API
                    if product_href not in product_cache:
                        product_response = requests.get(product_href, headers=headers)
                        product_response.raise_for_status()
                        product_data = product_response.json()
                        product_cache[product_href] = {
                            "code": product_data.get("code"),
                            "name": product_data.get("name"),
                            "category": product_data.get("productFolder", {}).get("name", "Uncategorized")
                        }
                    
                    product_info = product_cache[product_href]
                    
                    supply_positions.append({
                        "code": product_info["code"],
                        "name": product_info["name"],
                        "category": product_info["category"],
                        "quantity": float(position.get("quantity", 0))
                    })
                
                supplies.append({
                    "id": supply.get("id"),
                    "moment": supply.get("moment"),
                    "positions": supply_positions
                })
            
        except requests.HTTPError as e:
            print_api_errors(e.response)
            raise e
        
        offset += len(supply_rows)
        if len(supply_rows) < 100:
            break
    
    return supplies

def fetch_sales_channels(access_token: str) -> List[Dict]:
    """
    Fetches all sales channels from MoySklad.

    Args:
        access_token (str): Access token for authentication.

    Returns:
        List[Dict]: List of sales channels.
    """
    url = "https://api.moysklad.ru/api/remap/1.2/entity/saleschannel"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept-Encoding": "gzip"
    }
    sales_channels = []
    offset = 0
    limit = 1000

    while True:
        params = {"limit": limit, "offset": offset}
        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()
            sales_channels.extend(data.get("rows", []))
            if len(data.get("rows", [])) < limit:
                break
            offset += limit
        except requests.HTTPError as e:
            print_api_errors(e.response)
            raise e

    return sales_channels

def fetch_purchase_prices(access_token: str) -> Dict[str, float]:
    """
    Fetches purchase prices for all products.

    Args:
        access_token (str): Access token for authentication.

    Returns:
        Dict[str, float]: Dictionary mapping product codes to their purchase prices.
    """
    url = "https://api.moysklad.ru/api/remap/1.2/entity/product"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept-Encoding": "gzip"
    }
    purchase_prices = {}
    offset = 0
    limit = 1000

    while True:
        params = {"limit": limit, "offset": offset}
        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()
            for product in data.get("rows", []):
                code = product.get("code")
                buy_price = product.get("buyPrice", {}).get("value", 0.0) / 100  # Convert from kopeks to rubles
                if code:
                    purchase_prices[code] = buy_price
            if len(data.get("rows", [])) < limit:
                break
            offset += limit
        except requests.HTTPError as e:
            print_api_errors(e.response)
            raise e

    return purchase_prices

def fetch_customer_orders_for_current_day(access_token: str) -> List[Dict]:
    """
    Fetches all customer orders for the current day.

    Args:
        access_token (str): Access token for authentication.

    Returns:
        List[Dict]: List of customer orders.
    """
    url = "https://api.moysklad.ru/api/remap/1.2/entity/customerorder"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept-Encoding": "gzip"
    }
    today = datetime.now().strftime("%Y-%m-%d")
    params = {
        "filter": f"moment>={today} 00:00:00;moment<={today} 23:59:59",
        "limit": 1000,
        "expand": "positions,salesChannel"
    }
    orders = []
    offset = 0

    while True:
        params['offset'] = offset
        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()
            orders.extend(data.get("rows", []))
            if len(data.get("rows", [])) < params["limit"]:
                break
            offset += params["limit"]
        except requests.HTTPError as e:
            print_api_errors(e.response)
            raise e

    return orders

def generate_sales_report(access_token: str) -> Dict[str, Dict[str, Dict[str, float]]]:
    """
    Generates a sales report for the current day, categorized by status and sales channel.

    Args:
        access_token (str): Access token for authentication.

    Returns:
        Dict[str, Dict[str, Dict[str, float]]]: Nested dictionary with status as keys, sales channels as sub-keys,
                                      and total amounts as values.
    """
    sales_channels = fetch_sales_channels(access_token)
    channel_map = {channel['name']: channel for channel in sales_channels}

    purchase_prices = fetch_purchase_prices(access_token)
    orders = fetch_customer_orders_for_current_day(access_token)

    report = {
        "(Отгружено)": {},
        "(Доставляется)": {},
        "(Отменен, возврат)": {}
    }

    for order in orders:
        status = order.get("state", {}).get("name")
        if status not in report:
            continue  # Ignore other statuses

        sales_channel = order.get("salesChannel", {}).get("name", "Неизвестный канал")
        if sales_channel not in report[status]:
            report[status][sales_channel] = 0.0

        for position in order.get("positions", {}).get("rows", []):
            product_code = position.get("assortment", {}).get("code")
            quantity = float(position.get("quantity", 0))
            buy_price = purchase_prices.get(product_code, 0.0)
            total_cost = buy_price * quantity
            report[status][sales_channel] += total_cost

    return report

def fetch_orders_by_channels(access_token: str, status_channels: Dict[str, List[str]]) -> Dict[str, Dict[str, Dict[str, float]]]:
    # Get purchase prices for all products
    purchase_prices = fetch_purchase_prices(access_token)
    print("Loaded purchase prices:", purchase_prices)
    
    # Initialize results structure
    report = {}
    for status, channels in status_channels.items():
        if status == "(Отменен, возврат)":
            report["(Отменен)"] = {channel: {} for channel in channels}
            report["(Возврат)"] = {channel: {} for channel in channels}
            report[status] = {channel: {} for channel in channels}
        else:
            report[status] = {channel: {} for channel in channels}

    # Initialize caches
    product_cache = {}
    state_cache = {}
    channel_cache = {}
    
    # Prepare API request
    url = "https://api.moysklad.ru/api/remap/1.2/entity/customerorder"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept-Encoding": "gzip"
    }
    
    # Get current date and date 180 days ahead
    today = datetime.now()
    end_date = today + timedelta(days=180)
    
    params = {
        "filter": f"moment>={today.strftime('%Y-%m-%d')} 00:00:00;moment<={end_date.strftime('%Y-%m-%d')} 23:59:59",
        "limit": 1000,
        "expand": "positions,salesChannel,state"
    }
    
    # Fetch and process orders
    offset = 0
    while True:
        params['offset'] = offset
        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            orders = response.json().get("rows", [])
            
            if not orders:
                break
                
            for order in orders:
                # Get order date
                order_date = datetime.fromisoformat(order.get('moment', '').replace('Z', '+00:00'))
                order_date_str = order_date.strftime("%d.%m.%Y")
                
                # Get state name
                state_meta = order.get('state', {}).get('meta', {})
                state_href = state_meta.get('href')
                
                if state_href and state_href not in state_cache:
                    state_response = requests.get(state_href, headers=headers)
                    state_response.raise_for_status()
                    state_data = state_response.json()
                    state_cache[state_href] = state_data.get('name', '')
                
                state_name = state_cache.get(state_href, '')
                sheet_state_name = f"({state_name})"
                
                # Get channel name
                channel_meta = order.get('salesChannel', {}).get('meta', {})
                channel_href = channel_meta.get('href')
                
                if channel_href and channel_href not in channel_cache:
                    channel_response = requests.get(channel_href, headers=headers)
                    channel_response.raise_for_status()
                    channel_data = channel_response.json()
                    channel_cache[channel_href] = channel_data.get('name', '')
                
                channel_name = channel_cache.get(channel_href, '')
                
                print(f"\nProcessing order - State: {sheet_state_name}, Channel: {channel_name}")
                
                # Специальная обработка для отмененных и возвратных заказов
                if sheet_state_name in ["(Отменен)", "(Возврат)"]:
                    combined_state = "(Отменен, возврат)"
                    if combined_state in report and channel_name in report[combined_state]:
                        # Обработка позиций заказа
                        order_total = calculate_order_total(order, headers, product_cache, purchase_prices)
                        
                        # Обновляем значения с учетом даты
                        if order_total > 0:
                            report[sheet_state_name][channel_name][order_date_str] = \
                                report[sheet_state_name][channel_name].get(order_date_str, 0.0) + order_total
                            
                            # Обновляем общую сумму для объединенного статуса
                            report[combined_state][channel_name][order_date_str] = \
                                (report["(Отменен)"][channel_name].get(order_date_str, 0.0) + 
                                 report["(Возврат)"][channel_name].get(order_date_str, 0.0))
                else:
                    # Обычная обработка для других статусов
                    if sheet_state_name in report and channel_name in report[sheet_state_name]:
                        order_total = calculate_order_total(order, headers, product_cache, purchase_prices)
                        
                        if order_total > 0:
                            report[sheet_state_name][channel_name][order_date_str] = \
                                report[sheet_state_name][channel_name].get(order_date_str, 0.0) + order_total
            
            if len(orders) < params["limit"]:
                break
                
            offset += params["limit"]
            
        except requests.HTTPError as e:
            print_api_errors(e.response)
            raise e
    
    # Получаем остатки по категориям
    categories_costs = fetch_categories_costs(access_token)
    
    # Добавляем информацию об остатках в отчет
    report["Остатки"] = categories_costs
    
    # Удаляем временные ключи
    if "(Отменен)" in report:
        del report["(Отменен)"]
    if "(Возврат)" in report:
        del report["(Возврат)"]

    return report

def calculate_order_total(order, headers, product_cache, purchase_prices):
    """Helper function to calculate order total"""
    order_total = 0.0
    positions_meta = order.get("positions", {}).get("meta", {})
    positions_href = positions_meta.get("href")
    
    if positions_href:
        positions_response = requests.get(positions_href, headers=headers)
        positions_response.raise_for_status()
        positions = positions_response.json().get("rows", [])
        
        for position in positions:
            product_href = position.get("assortment", {}).get("meta", {}).get("href")
            
            if product_href not in product_cache:
                product_response = requests.get(product_href, headers=headers)
                product_response.raise_for_status()
                product_data = product_response.json()
                product_cache[product_href] = product_data.get("code")
                
            product_code = product_cache[product_href]
            
            if product_code:
                quantity = float(position.get("quantity", 0))
                buy_price = purchase_prices.get(product_code, 0.0)
                position_cost = buy_price * quantity
                order_total += position_cost
    
    return order_total

def fetch_categories_costs(access_token: str) -> Dict[str, float]:
    china_transit_url = fetch_url_stock_CHINA_in_transit(access_token)
    """
    Получает суммарную себестоимость товаров по категориям
    """
    url = "https://api.moysklad.ru/api/remap/1.2/report/stock/all"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept-Encoding": "gzip"
    }
    
    # Инициализируем словарь для хранения сумм по категориям
    categories_total = {"Всего": 0.0}  # Только общая сумма изначально
    
    offset = 0
    limit = 1000
    
    while True:
        params = {
            "limit": limit,
            "offset": offset,
            "groupBy": "product",
            "filter": f"store!={china_transit_url}"
        }
        
        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()
            
            rows = data.get("rows", [])
            if not rows:
                break
                
            for item in rows:
                stock = float(item.get("stock", 0))
                price = float(item.get("price", 0)) / 100  # Convert from kopeks to rubles
                total_cost = stock * price
                
                # Получаем категорию товара
                folder = item.get("folder", {})
                category_name = folder.get("name", "Без категории") if folder else "Без категории"
                
                # Если категория встречается впервые, добавляем её в словарь
                if category_name not in categories_total:
                    categories_total[category_name] = 0.0
                
                # Добавляем стоимость в соответствующую категорию
                categories_total[category_name] += total_cost
                    
                # Добавляем в общую сумму
                categories_total["Всего"] += total_cost
            
            if len(rows) < limit:
                break
                
            offset += limit
            
        except requests.HTTPError as e:
            print_api_errors(e.response)
            raise e
    
    print(f"Found categories with total costs: {categories_total}")
    return categories_total

def fetch_url_stock_CHINA_in_transit(access_token: str) -> str:
    """
    Fetches all purchase orders with status "В пути" for the current date.

    Args:
        access_token (str): MoySklad API access token.

    Returns:
        str: URL of the store with name "В ПУТИ ИЗ КИТАЯ"
    """
    url = "https://api.moysklad.ru/api/remap/1.2/entity/store/"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept-Encoding": "gzip"
    }

    params = {
        "filter": f"name=В ПУТИ ИЗ КИТАЯ"
    }

    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    data = response.json()
    rows = data.get("rows", [])
    
    if not rows:
        raise ValueError("Store 'В ПУТИ ИЗ КИТАЯ' not found")
        
    store = rows[0]  # Get first matching store
    store_url = store.get("meta", {}).get("href")
    
    if not store_url:
        raise ValueError("Store URL not found in response")
        
    return store_url

def fetch_stock_CHINA_in_transit(access_token: str) -> Dict[str, int]:
    store_url = fetch_url_stock_CHINA_in_transit(access_token)
    url = f"https://api.moysklad.ru/api/remap/1.2/report/stock/bystore"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept-Encoding": "gzip"
    }

    params = {
        "filter": f"store={store_url}"
    }

    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    stock_data = response.json()

    category_totals = {}

    for row in stock_data.get('rows', []):
        product_href = row.get('meta').get('href')
        
        # Fetch product details
        product_response = requests.get(product_href, headers=headers)
        product_response.raise_for_status()
        product_details = product_response.json()

        category_name = product_details.get('pathName', 'Unknown')
        buy_price = product_details.get('buyPrice', {}).get('value', 0.0) / 100  # Convert from kopeks to rubles

        # Calculate total value for each store
        for store in row.get('stockByStore', []):
            store_quantity = store.get('stock')
            store_total = store_quantity * buy_price

            # Add to category totals
            if category_name in category_totals:
                category_totals[category_name] += store_total
            else:
                category_totals[category_name] = store_total


    return category_totals
