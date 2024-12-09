from typing import List, Dict
import requests

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
    # Фильтруем коды товаров, оставляя только те, для которых нет информации
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
    
    stock_dict = {}
    
    try:
        response = requests.get(url, headers=headers)
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