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
    Получает информацию о товарах и комплектах.
    """
    # Фильтруем коды, оставляя только те, для которых нет информации
    new_codes = [code for code in product_codes if code not in existing_products]
    
    if not new_codes:
        return existing_products
    
    products_dict = existing_products.copy()
    not_found_codes = []  # Коды, которые не найдены среди товаров
    
    # Сначала ищем среди товаров
    url = "https://api.moysklad.ru/api/remap/1.2/entity/product"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept-Encoding": "gzip"
    }
    
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
                    "category": product.get("pathName", "Без категории"),
                    "description": product.get("description", ""),
                    "article": product.get("article", ""),
                    "meta": product.get("meta"),
                    "type": "product"  # Добавляем тип для различения
                }
            else:
                not_found_codes.append(code)
        except requests.HTTPError as e:
            print_api_errors(e.response)
            not_found_codes.append(code)
            continue
    
    # Если остались ненайденные коды, ищем их среди комплектов
    if not_found_codes:
        bundle_url = "https://api.moysklad.ru/api/remap/1.2/entity/bundle"
        
        for code in not_found_codes:
            try:
                params = {"filter": f"code={code}"}
                response = requests.get(bundle_url, headers=headers, params=params)
                response.raise_for_status()
                data = response.json()
                
                if data.get("rows"):
                    bundle = data["rows"][0]
                    products_dict[code] = {
                        "id": bundle.get("code"),
                        "name": bundle.get("name"),
                        "category": bundle.get("pathName", "Без категории"),
                        "description": bundle.get("description", ""),
                        "article": bundle.get("article", ""),
                        "meta": bundle.get("meta"),
                        "type": "bundle"  # Добавляем тип для различения
                    }
                else:
                    print(f"Code {code} not found in both products and bundles")
            except requests.HTTPError as e:
                print_api_errors(e.response)
                print(f"Error fetching bundle with code {code}")
                continue
    
    return products_dict

def fetch_customer_orders_for_products(access_token: str, start_date: str, end_date: str, products: Dict[str, Dict]) -> List[Dict]:
    
    url = "https://api.moysklad.ru/api/remap/1.2/entity/customerorder"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept-Encoding": "gzip"
    }
    
    params = {
        "filter": f"moment>={start_date};moment<={end_date}",
        "limit": 1000,
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
                positions = order.get("positions", {})
                positions_href = positions.get("meta", {}).get("href")
                positions_response = requests.get(positions_href, headers=headers)
                positions_response.raise_for_status()
                positions_data = positions_response.json()
                positions_rows = positions_data.get("rows", [])

                for position in positions_rows:
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
        print(result)
    
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
        "filter": f"store!={china_transit_url}",
        "limit": 1000
    }
    
    stock_dict = {}
    offset = 0
    
    while True:
        params['offset'] = offset
        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()
            rows = data.get("rows", [])
            
            if not rows:
                break
            
            for item in rows:
                code = item.get("code")
                if code in product_codes:
                    stock_dict[code] = float(item.get("stock", 0))
            
            if len(rows) < params["limit"]:
                break
                
            offset += len(rows)
                
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
        "filter": f"moment>{start_date} 00:00:00;store!={china_transit_url}",
        "limit": 1000,
        #"expand": "positions",
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
                positions = supply.get("positions", {})
                positions_href = positions.get("meta", {}).get("href")
                positions_response = requests.get(positions_href, headers=headers)
                positions_response.raise_for_status()
                positions_data = positions_response.json()
                positions_rows = positions_data.get("rows", [])
                supply_positions = []
                
                for position in positions_rows:
                    print(position)
                    position_href = position.get("meta", {}).get("href")
                    position_response = requests.get(position_href, headers=headers)
                    position_response.raise_for_status()
                    position_data = position_response.json()
                    assortment = position_data.get("assortment", {})
                    product_href = assortment.get("meta", {}).get("href")
                    
                    # Получаем информацию о товаре из кэша или через API
                    if product_href not in product_cache:
                        product_response = requests.get(product_href, headers=headers)
                        product_response.raise_for_status()
                        product_data = product_response.json()
                        print(product_data)
                        product_cache[product_href] = {
                            "code": product_data.get("code"),
                            "name": product_data.get("name"),
                            "category": product_data.get("pathName", "Uncategorized")
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
        if len(supply_rows) < params["limit"]:
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
    # Initialize results structure
    report = {}
    for status, channels in status_channels.items():
        if status == "(Отменен, возврат)":
            report["(Отменен)"] = {channel: {} for channel in channels}
            report["(Возврат)"] = {channel: {} for channel in channels}
            report[status] = {channel: {} for channel in channels}
        else:
            report[status] = {channel: {} for channel in channels}

    # Prepare API request
    url = "https://api.moysklad.ru/api/remap/1.2/entity/customerorder"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept-Encoding": "gzip"
    }
    
    today = datetime.now()
    end_date = today - timedelta(days=90)
    
    params = {
        "filter": f"moment<={today.strftime('%Y-%m-%d')} 00:00:00;moment>={end_date.strftime('%Y-%m-%d')} 23:59:59",
        "limit": 100,  # Используем limit=100 для работы expand
        "expand": "positions,positions.assortment,positions.assortment.components,state,salesChannel"
    }
    
    offset = 0
    while True:
        params['offset'] = offset
        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            orders = response.json().get("rows", [])
            
            if not orders:
                break
            
            # Собираем все уникальные href'ы товаров из заказов
            product_hrefs = set()
            for order in orders:
                for position in order.get("positions", {}).get("rows", []):
                    assortment = position.get("assortment", {})
                    if assortment.get("meta", {}).get("type") == "product":
                        product_href = clean_href(assortment.get("meta", {}).get("href"))
                        product_hrefs.add(product_href)  # Очищаем href перед добавлением
                    elif assortment.get("meta", {}).get("type") == "bundle":
                        for component in assortment.get("components", {}).get("rows", []):
                            component_href = clean_href(component.get("assortment", {}).get("meta", {}).get("href"))
                            product_hrefs.add(component_href)  # Очищаем href перед добавлением
            # Получаем себестоимость всех товаров одним запросом
            costs_cache = get_products_stock_costs(list(product_hrefs), access_token)
            print(f"cost_caches : {costs_cache}")
                
            for order in orders:
                order_date = datetime.fromisoformat(order.get('moment', '').replace('Z', '+00:00'))
                order_date_str = order_date.strftime("%d.%m.%Y")
                
                state_name = order.get('state', {}).get('name', '')
                sheet_state_name = f"({state_name})"
                
                channel_name = order.get('salesChannel', {}).get('name', '')
                
                if sheet_state_name in ["(Отменен)", "(Возврат)"]:
                    combined_state = "(Отменен, возврат)"
                    if combined_state in report and channel_name in report[combined_state]:
                        order_total = int(calculate_order_totals(order, costs_cache))
                        
                        if order_total > 0:
                            report[sheet_state_name][channel_name][order_date_str] = \
                                report[sheet_state_name][channel_name].get(order_date_str, 0.0) + order_total
                            
                            report[combined_state][channel_name][order_date_str] = \
                                (report["(Отменен)"][channel_name].get(order_date_str, 0.0) + 
                                 report["(Возврат)"][channel_name].get(order_date_str, 0.0))
                else:
                    if sheet_state_name in report and channel_name in report[sheet_state_name]:
                        order_total = int(calculate_order_totals(order, costs_cache))
                        #print(f"order total - {order_total}")
                        
                        if order_total > 0:
                            report[sheet_state_name][channel_name][order_date_str] = \
                                report[sheet_state_name][channel_name].get(order_date_str, 0.0) + order_total
                            
            offset += params["limit"]
            
        except requests.HTTPError as e:
            print_api_errors(e.response)
            raise e
    
    if "(Отменен)" in report:
        del report["(Отменен)"]
    if "(Возврат)" in report:
        del report["(Возврат)"]

    current_date = datetime.now().strftime("%d.%m.%Y")
    summarized_report = summarize_orders(report, current_date)

    return report

def clean_href(href: str) -> str:
    """Очищает href от параметров запроса"""
    return href.split('?')[0]

def get_products_stock_costs(product_hrefs: List[str], access_token: str) -> Dict[str, float]:
    """
    Получает себестоимость для списка товаров батчами
    """
    BATCH_SIZE = 100
    stock_url = "https://api.moysklad.ru/api/remap/1.2/report/stock/all"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept-Encoding": "gzip"
    }
    
    # Очищаем href'ы от параметров expand
    clean_hrefs = [clean_href(href) for href in product_hrefs]
    all_costs = {}

    valid_hrefs = []
    for href in clean_hrefs:
        if "product" in href or "bundle" in href:
            valid_hrefs.append(href)
    
    for i in range(0, len(valid_hrefs), BATCH_SIZE):
        batch = valid_hrefs[i:i + BATCH_SIZE]
        products_filter = ";".join(f"product={href}" for href in batch)
        
        try:
            response = requests.get(stock_url, headers=headers, params={"filter": products_filter})
            response.raise_for_status()
            if response.status_code == 200:
                data = response.json()
                batch_costs = {
                    clean_href(row.get("meta", {}).get("href", "")): row.get("price", 0.0) / 100
                    for row in data.get("rows", [])
                }
                all_costs.update(batch_costs)
        except requests.RequestException as e:
            raise e
        
    return all_costs

def calculate_order_totals(order, costs_cache: Dict[str, float]):
    """
    Calculate order total using costs cache
    """
    order_total = 0.0
    positions = order.get("positions", {}).get("rows", [])
    
    for position in positions:    
        assortment = position.get("assortment", {})
        assortment_type = assortment.get("meta", {}).get("type")
        quantity = position.get("quantity", 0)
        
        if assortment_type == "product":
            product_href = clean_href(assortment.get("meta", {}).get("href"))
            buy_price = costs_cache.get(product_href, 0.0)
            #print(f"Product href: {product_href}, Buy price: {buy_price}, Quantity: {quantity}")
            order_total += buy_price * quantity
            #print(f"Product cost = {order_total}")
                
        elif assortment_type == "bundle":
            components = assortment.get("components", {}).get("rows", [])
            bundle_cost = 0.0
            
            for component in components:
                comp_quantity = component.get("quantity", 1)
                product_href = clean_href(component.get("assortment", {}).get("meta", {}).get("href"))
                buy_price = costs_cache.get(product_href, 0.0)
                #print(f"Component href: {product_href}, Buy price: {buy_price}, Component quantity: {comp_quantity}")
                bundle_cost += buy_price * comp_quantity
            
            order_total += bundle_cost * quantity
            
    return order_total

def summarize_orders(report: Dict[str, Dict[str, Dict[str, float]]], current_date: str) -> Dict[str, Dict[str, Dict[str, float]]]:
    """
    Summarizes the order prices for each channel and status for the current day over the last three months.
    
    Args:
        report: The original report containing order prices.
        current_date: The current date in the format "dd.mm.yyyy".
    
    Returns:
        A new report with summed prices for each channel and status for the current day over the last three months.
    """
    # Convert current_date to a datetime object
    current_date_obj = datetime.strptime(current_date, "%d.%m.%Y")
    three_months_ago = current_date_obj - timedelta(days=90)

    # Initialize a new report for summarized data
    summarized_report = {}

    # Initialize the report for the current date
    date_str = current_date_obj.strftime("%d.%m.%Y")
    summarized_report[date_str] = {}

    # Debug: Check the contents of the report
    print(f"Initial report data: {report}")  # Debug message

    for status, channels in report.items():
        print(f"Processing status: {status}")  # Debug message
        for channel, date_amounts in channels.items():
            print(f"Processing channel: {channel}")  # Debug message
            # Initialize the channel and status in the summarized report
            if status not in summarized_report[date_str]:
                summarized_report[date_str][status] = {}
            if channel not in summarized_report[date_str][status]:
                summarized_report[date_str][status][channel] = 0.0

            # Sum the amounts for the current day over the last three months
            for order_date_str, amount in date_amounts.items():
                order_date_obj = datetime.strptime(order_date_str, "%d.%m.%Y")
                # Check if the order date is within the last three months
                if three_months_ago <= order_date_obj <= current_date_obj:
                    summarized_report[date_str][status][channel] += amount
                    print(f"Adding {amount} to {status} for {channel} on {date_str}")  # Debug message

    print(f"Summarized report: {summarized_report}")  # Debug message
    return summarized_report

def calculate_order_total(order, access_token: str):
    """
    Calculate order total considering both products and bundles
    """
    order_total = 0.0
    positions = order.get("positions", {}).get("rows", [])
    
    for position in positions:    
        assortment = position.get("assortment", {})
        assortment_type = assortment.get("meta", {}).get("type")
        quantity = position.get("quantity", 0)
        
        if assortment_type == "product":
            product_href = assortment.get("meta", {}).get("href")
            buy_price = get_product_stock_cost(product_href, access_token)
            order_total += buy_price * quantity
                
        elif assortment_type == "bundle":
            components = assortment.get("components", {}).get("rows", [])
            bundle_cost = 0.0
            
            for component in components:
                comp_quantity = component.get("quantity", 1)
                product_href = component.get("assortment", {}).get("meta", {}).get("href")
                buy_price = get_product_stock_cost(product_href, access_token)
                bundle_cost += buy_price * comp_quantity
            
            order_total += bundle_cost * quantity

    return order_total

def get_product_stock_cost(product_href, access_token: str):

    stock_url = "https://api.moysklad.ru/api/remap/1.2/report/stock/all"
    params = {"filter": f"product={product_href}"}
    
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept-Encoding": "gzip"
    }
    try:
        response = requests.get(stock_url, headers=headers, params=params)
        response.raise_for_status()
        if response.status_code == 200:
            data = response.json()
            rows = data.get("rows", [])
            if rows:
                return rows[0].get("price", 0.0)/100
    except requests.RequestException:
        pass

    return 0.0

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
    
    categories_total = {"Всего": 0.0}
    
    offset = 0
    limit = 1000
    total_records = None
    
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
            
            # Получаем общее количество записей при первом запросе
            if total_records is None:
                total_records = data.get("meta", {}).get("size", 0)
                print(f"Total records to process: {total_records}")
            
            rows = data.get("rows", [])
            if not rows:
                break
            
            print(f"Processing batch: {offset + 1}-{offset + len(rows)} of {total_records}")
                
            for item in rows:
                stock = float(item.get("stock", 0))
                price = float(item.get("price", 0)) / 100
                total_cost = stock * price
                
                folder = item.get("folder", {})
                category_name = folder.get("name", "Без категории") if folder else "Без категории"
                category_path_name = folder.get("pathName", category_name) if folder else category_name
                
                if category_name not in categories_total:
                    categories_total[category_name] = 0.0

                if category_path_name not in categories_total:
                    categories_total[category_path_name] = 0.0
                
                categories_total[category_name] += total_cost
                if category_name != category_path_name:
                    categories_total[category_path_name] += total_cost
                categories_total["Всего"] += total_cost
            
            # Проверяем, получены ли все записи
            if offset + len(rows) >= total_records:
                print("All records processed successfully")
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
        buy_price = product_details.get('buyPrice', {}).get('value', 0.0) / 100

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

def calculate_costs_by_status_and_channel(access_token: str, status_channels: Dict[str, List[str]]) -> Dict[str, Dict[str, Dict[str, float]]]:
    """
    Calculates the total cost of ordered products over the last three months, grouped by status and sales channel.

    Args:
        access_token (str): Access token for authentication.
        status_channels (Dict[str, List[str]]): Dictionary mapping statuses to lists of sales channels.

    Returns:
        Dict[str, Dict[str, Dict[str, float]]]: Total costs grouped by status and sales channel.
    """
    # Инициализация отчета
    report = {}
    for status, channels in status_channels.items():
        report[status] = {channel: {} for channel in channels}

    today = datetime.now()
    end_date = today - timedelta(days=90)  # 3 months ago

    # Подготовка API запроса
    url = "https://api.moysklad.ru/api/remap/1.2/entity/customerorder"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept-Encoding": "gzip"
    }

    params = {
        "filter": f"moment<={today.strftime('%Y-%m-%d')} 23:59:59;moment>={end_date.strftime('%Y-%m-%d')} 00:00:00",
        "limit": 100,
        "expand": "positions,positions.assortment,positions.assortment.components,state,salesChannel"
    }

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
                state_name = order.get('state', {}).get('name', '')
                channel_name = order.get('salesChannel', {}).get('name', 'Неизвестный канал')

                # Проверяем, есть ли статус и канал в report
                if state_name in report and channel_name in report[state_name]:
                    order_total = calculate_order_totals(order, get_products_stock_costs(
                        [clean_href(position.get("assortment", {}).get("meta", {}).get("href")) for position in order.get("positions", {}).get("rows", [])],
                        access_token
                    ))

                    # Убедимся, что order_total является числом
                    if isinstance(order_total, (int, float)):
                        if 'total' not in report[state_name][channel_name]:
                            report[state_name][channel_name]['total'] = 0.0
                        report[state_name][channel_name]['total'] += order_total

            offset += params["limit"]

        except requests.HTTPError as e:
            print_api_errors(e.response)
            raise e

    return report


